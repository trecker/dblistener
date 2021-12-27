package dblistener

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

// IDBListener - Интерфейс слушателя базы данных
type IDBListener interface {
	// Run Запуск
	Run() error
	// Stop Остановка
	Stop()
	// GetState Получить текущее состояние слушателя
	GetState() (state StateListener)

	// SetConnection установить новое подключение
	SetConnection(conn *pgx.Conn)
	// SetLostConnectionCallback Установить callback при потери соединения
	SetLostConnectionCallback(callback func(sender IDBListener))

	// OpenChannel открытие каналов уведомлений
	OpenChannel(channelNames ...string) (err error)
	// CloseChannel закрытие канала уведомления
	CloseChannel(channelName ...string) (err error)
	// IsChannelExist проверка существования
	IsChannelExist(channelName string) (exist bool)
	// CloseAllChannels закрыть все каналы
	CloseAllChannels() (err error)

	// AddHandler Добавление функции обработчика, срабатывает при получении уведомления от базы данных
	AddHandler(name string, handler func(payLoad interface{})) (err error)
	// RemoveHandler Удаление функции обработчика
	RemoveHandler(name string) (err error)
	// IsHandlerExist Проверка существования обработчика
	IsHandlerExist(name string) (exist bool)

	// SetLogger Установить логер
	SetLogger(logger *logrus.Logger)
}

// StateListener - состояние слушателя
type StateListener int

const (
	// StateStop Listener остановлен или не запущен
	StateStop = iota
	// StateInit Listener запущен, но проходит подготовительный процесс, открываются каналы и т.д.
	StateInit
	// StateRunning Listener запущен и работает в штатном режиме
	StateRunning
	// StateWaitNewConnection Listener запущен и ожидает нового подключения
	StateWaitNewConnection
	// StateOpenCloseChannel состояние при открытии/закрытии каналов, прослушивание остановлено
	StateOpenCloseChannel
)

type Listener struct {
	conn                    *pgx.Conn                // Соединение с базой данных
	context                 context.Context          // Контекст для ожидания уведомлений от базы
	stopWaitingNotification func()                   // Функция для остановки ожидания уведомлений от базы
	lostConnectionCallback  func(sender IDBListener) // Callback при потери связи
	logger                  *logrus.Logger

	cm            sync.Mutex
	channels      []string        // слайс имён каналов к которым подсоединён слушатель
	queueChannels map[string]bool // очередь каналов на подключение, если соединение было потеряно

	hm                   sync.Mutex
	notificationHandlers map[string]func(payLoad interface{}) // Обработчики уведомлений

	stopChan   chan bool // Канал для остановки слушателя
	newConnect chan bool // Канал для обновления подключения

	state       StateListener // Текущее состояние слушателя
	sendCommand bool
}

func New(connect *pgx.Conn) *Listener {
	ctx, stopFunc := context.WithCancel(context.Background())

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	return &Listener{
		conn:                    connect,
		context:                 ctx,
		stopWaitingNotification: stopFunc,
		lostConnectionCallback:  func(sender IDBListener) {},
		logger:                  logger,

		cm:            sync.Mutex{},
		channels:      []string{},
		queueChannels: map[string]bool{},

		hm:                   sync.Mutex{},
		notificationHandlers: map[string]func(payLoad interface{}){},

		stopChan:   make(chan bool),
		newConnect: make(chan bool),

		state:       StateStop,
		sendCommand: false,
	}
}

var (
	ErrorEmptyHandlerName    = errors.New("empty handler name")
	ErrorHandlerAlreadyExist = errors.New("handler already exist")
	ErrorHandlerIsNil        = errors.New("handler function is nil")
	ErrorHandlerNotExist     = errors.New("handler not exist")
	ErrorEmptyChannelName    = errors.New("empty channel name")
	ErrorChannelNotExist     = errors.New("channel not exist")
)

// Run - запуск слушателя. Подключается к каналам
func (l *Listener) Run() error {
	if l.state != StateStop {
		err := fmt.Errorf("the listener '%s' is already running", l.conn.Config().Database)
		l.logger.Error(err)
		return err
	}

	l.state = StateInit
	go func() {
		defer func() {
			l.state = StateStop
		}()

		l.logger.Infof("Database '%s' listening started ", l.conn.Config().Database)
		defer l.logger.Infof("Database '%s' listening stopped", l.conn.Config().Database)

		// Подключение ко всем каналам
		l.cm.Lock()
		err := l.openAllChanel()
		l.cm.Unlock()
		if err != nil {
			l.logger.Errorf("database '%s' listener open all channels err: %s", l.conn.Config().Database, err)
			return
		}

		// Основной цикл
		for {
			l.state = StateOpenCloseChannel
			l.cm.Lock()
			l.logger.Debugln("start pipe queue")
			// Открытие или закрытие каналов в очереди
			l.queuePipe()
			l.cm.Unlock()

			l.logger.Debugln("wait notification")
			l.state = StateRunning
			notification, err := l.conn.WaitForNotification(l.context)
			if err != nil {
				if l.context.Err() == context.Canceled {
					if l.sendCommand {
						l.logger.Trace("send command")
						l.context, l.stopWaitingNotification = context.WithCancel(context.Background())
						l.sendCommand = false
						continue
					} else {
						l.logger.Trace("stop listener")
						l.stopChan <- true
						return
					}
				}

				if l.conn.IsClosed() {
					l.state = StateWaitNewConnection
					l.logger.Infof("database '%s' listener connect is closed: %s", l.conn.Config().Database, err)
					l.logger.Infof("database '%s' listener waiting for a new connection", l.conn.Config().Database)
					go l.lostConnectionCallback(l)
					err = l.waitingConnection()
					if err != nil {
						return
					}
					continue
				}

				l.logger.Errorf("database '%s' listener waiting for notification error: %s", l.conn.Config().Database, err)
				return
			}

			l.hm.Lock()
			l.handle(notification)
			l.hm.Unlock()

			l.logger.Debugln("database listener", l.conn.Config().Database, "PID:", notification.PID, "Channel:", notification.Channel)
		}
	}()

	return nil
}

// handle - вызов обработчиков входящих сообщений, обработчики вызываются в отдельных потоках
func (l *Listener) handle(notification *pgconn.Notification) {
	for _, handler := range l.notificationHandlers {
		go handler(&notification.Payload)
	}
}

func (l *Listener) queuePipe() {
	cns := make([]string, 0, len(l.queueChannels))
	for k := range l.queueChannels {
		cns = append(cns, k)
	}

	for _, chanName := range cns {
		open := l.queueChannels[chanName]
		if open {
			err := l.openNotificationChannel(chanName)
			if err != nil {
				l.logger.Errorf("open channel '%s' error: %s", chanName, err)
				continue
			}
			l.removeChannelQueue(chanName)
			if !l.isChannelExist(chanName) {
				l.addChannel(chanName)
			}
		} else {
			err := l.closeNotificationChannel(chanName)
			if err != nil {
				l.logger.Errorf("close channel '%s' error: %s", chanName, err)
				continue
			}
			l.removeChannelQueue(chanName)
			if l.isChannelExist(chanName) {
				l.removeChannel(chanName)
			}
		}
	}
}

func (l *Listener) waitingConnection() error {
	// Ожидание нового подключения
	isNewConnection := <-l.newConnect
	if isNewConnection {
		l.logger.Infof("database '%s' listener new connection established", l.conn.Config().Database)
		// Так как соединение новое, то значит каналы не открыты, надо открыть
		l.moveAllChannelsToQueue(true)
	}
	return nil
}

// Stop - остановка слушателя
func (l *Listener) Stop() {
	l.logger.Infoln("Start stopping listener")
	l.logger.Debugf("current state: %d", l.state)

	l.sendCommand = false
	l.stopWaitingNotification()

	if l.state == StateWaitNewConnection {
		l.logger.Trace("stop: waiting new connection")
		l.newConnect <- false
	}

	// Ожидание остановки при условии что запущено прослушивание
	if l.state != StateStop {
		l.logger.Traceln("stop: state stop")
		<-l.stopChan
	}

	l.logger.Infoln("Listener is stopped")
}

// SetConnection - установить новое соединение.
func (l *Listener) SetConnection(conn *pgx.Conn) {
	l.logger.Infof("New connection: host=%s port=%d db_name=%s user=%s",
		conn.Config().Host, conn.Config().Port, conn.Config().Database, conn.Config().User)
	switch l.state {
	case StateWaitNewConnection:
		l.logger.Debugln("listener state is: StateWaitNewConnection")
		l.conn = conn
		l.newConnect <- true
	default:
		l.conn = conn
	}
}

// SetLogger - установить логер для слушателя
func (l *Listener) SetLogger(logger *logrus.Logger) {
	l.logger = logger
}

// IsChannelExist - проверка открыт канал или нет
func (l *Listener) IsChannelExist(channelName string) bool {
	l.cm.Lock()
	defer l.cm.Unlock()

	if l.isChannelExist(channelName) {
		return true
	}

	if l.isChannelExistInQueue(channelName) {
		return l.queueChannels[channelName]
	}

	return false
}

// OpenChannel - открыть каналы оповещения и добавить их в массив
func (l *Listener) OpenChannel(channelNames ...string) error {
	l.cm.Lock()
	defer l.cm.Unlock()

	for _, channelName := range channelNames {
		err := l.openChannel(channelName)
		if err != nil {
			return err
		}
	}

	if l.state == StateRunning {
		l.sendCommand = true
		l.stopWaitingNotification()
	}

	return nil
}

func (l *Listener) openChannel(channelName string) error {
	if channelName == "" {
		return ErrorEmptyChannelName
	}

	if l.isChannelExist(channelName) {
		l.logger.Debugf("channel '%s' already open", channelName)
		return nil
	}

	if l.isChannelExistInQueue(channelName) {
		open := l.queueChannels[channelName]
		if open {
			l.logger.Debugf("channel '%s' already in queue", channelName)
			return nil
		}
	}

	l.addChannelToQueue(channelName, true)
	return nil
}

// CloseChannel - закрыть канал оповещения и удалить его из массива
func (l *Listener) CloseChannel(channelName ...string) error {
	for _, channel := range channelName {
		err := l.closeChannel(channel)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *Listener) closeChannel(channelName string) error {
	if channelName == "" {
		return ErrorEmptyChannelName
	}

	l.cm.Lock()
	if l.isChannelExist(channelName) {
		l.addChannelToQueue(channelName, false)
	} else {
		if l.isChannelExistInQueue(channelName) {
			l.removeChannelQueue(channelName)
		} else {
			l.cm.Unlock()
			l.logger.Debugln(ErrorChannelNotExist)
			return nil
		}
	}
	l.cm.Unlock()

	if l.state == StateRunning {
		l.sendCommand = true
		l.stopWaitingNotification()
	}

	return nil
}

func (l *Listener) closeNotificationChannel(channelName string) error {
	_, err := l.conn.Exec(l.context, fmt.Sprintf("unlisten %s", channelName))
	if err != nil {
		l.logger.Errorf("Error unlistening channel %s: %s", channelName, err)
		return err
	}

	return nil
}

// CloseAllChannels - Закрыть все каналы и удалить их из массива
func (l *Listener) CloseAllChannels() error {
	l.cm.Lock()
	defer l.cm.Unlock()
	l.logger.Traceln("Close all channels")

	cns := make([]string, 0, len(l.queueChannels))
	for k := range l.queueChannels {
		cns = append(cns, k)
	}

	for _, channel := range cns {
		if !l.isChannelExist(channel) {
			l.removeChannelQueue(channel)
		}
	}

	for _, channel := range l.channels {
		l.queueChannels[channel] = false
	}

	if l.state == StateRunning {
		l.sendCommand = true
		l.stopWaitingNotification()
	}
	return nil
}

// IsHandlerExist - Проверка существования обработчика
func (l *Listener) IsHandlerExist(name string) bool {
	l.hm.Lock()
	defer l.hm.Unlock()

	return l.isHandlerExist(name)
}

// AddHandler - Добавление обработчика
func (l *Listener) AddHandler(name string, handler func(payLoad interface{})) error {
	if name == "" {
		return ErrorEmptyHandlerName
	}

	l.hm.Lock()
	exist := l.isHandlerExist(name)
	l.hm.Unlock()
	if exist {
		return ErrorHandlerAlreadyExist
	}

	if handler == nil {
		return ErrorHandlerIsNil
	}

	l.hm.Lock()
	l.addHandler(name, handler)
	l.hm.Unlock()

	return nil
}

// RemoveHandler - Удаление обработчика
func (l *Listener) RemoveHandler(name string) error {
	if name == "" {
		return ErrorEmptyHandlerName
	}

	l.hm.Lock()
	defer l.hm.Unlock()

	if !l.isHandlerExist(name) {
		return ErrorHandlerNotExist
	}

	l.removeHandler(name)
	return nil
}

// SetLostConnectionCallback - установить обработчик при потери соединения с базой данных
func (l *Listener) SetLostConnectionCallback(callback func(sender IDBListener)) {
	l.lostConnectionCallback = callback
}

// GetState - получить состояние слушателя
func (l *Listener) GetState() StateListener {
	return l.state
}

// isChannelExist - проверка что канал существует в массиве
func (l *Listener) isChannelExist(channelName string) bool {
	for _, name := range l.channels {
		if name == channelName {
			return true
		}
	}

	return false
}

// isChannelExistInQueue - проверка что канал существует в очереди
func (l *Listener) isChannelExistInQueue(channelName string) bool {
	_, ok := l.queueChannels[channelName]
	return ok
}

// addChannel - добавить канал в массив
func (l *Listener) addChannel(channelName string) {
	l.channels = append(l.channels, channelName)
}

// addChannelToQueue - добавить канал в очередь
func (l *Listener) addChannelToQueue(channelName string, open bool) {
	l.queueChannels[channelName] = open
}

// removeChannel - удаление канала из массива
func (l *Listener) removeChannel(channelName string) {
	for i, name := range l.channels {
		if name == channelName {
			l.channels = append(l.channels[:i], l.channels[i+1:]...)
			break
		}
	}
}

// removeChannelQueue - удаление канала из очереди
func (l *Listener) removeChannelQueue(channelName string) {
	_, ok := l.queueChannels[channelName]
	if ok {
		delete(l.queueChannels, channelName)
	}
}

// isHandlerExist - проверка существования обработчика в словаре обработчиков
func (l *Listener) isHandlerExist(name string) bool {
	return l.notificationHandlers[name] != nil
}

// addHandler - добавить обработчик принятых сообщений в словарь
func (l *Listener) addHandler(name string, handler func(payLoad interface{})) {
	l.notificationHandlers[name] = handler
}

// removeHandler - удалить обработчик из словаря
func (l *Listener) removeHandler(name string) {
	delete(l.notificationHandlers, name)
}

// openNotificationChannel - подписаться на сообщения из канала channelName
func (l *Listener) openNotificationChannel(channelName string) error {
	l.logger.Infof("Open channel: %s", channelName)

	_, err := l.conn.Exec(l.context, fmt.Sprintf("listen %s", channelName))
	if err != nil {
		l.logger.Errorf("Error listening channel %s: %s", channelName, err)
		return err
	}

	return nil
}

// openAllChanel - открыть все каналы. Может понадобится при замене подключения
func (l *Listener) openAllChanel() error {
	l.logger.Traceln("open all channel")

	for _, channelName := range l.channels {
		if err := l.openNotificationChannel(channelName); err != nil {
			return err
		}
	}
	return nil
}

func (l *Listener) moveAllChannelsToQueue(open bool) {
	l.logger.Traceln("move all channels to queue")

	for _, channel := range l.channels {
		l.addChannelToQueue(channel, open)
	}

	l.channels = []string{}
}
