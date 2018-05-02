# -*-coding:utf8-*-
import threading
from twisted.internet import reactor
from autobahn.twisted.websocket import listenWS


class CommunicationTool:

    Server = 1
    Client = 0

    def __init__(self):
        self.isAutoReconnect = False

    # 设置创建的Server还是Client
    def setFlag(self, flag):
        self.flag = flag

    # 创建server
    def createServer(self, addr, port):
        # 先判断创建的类型
        if self._isServer():
            if isinstance(addr, str) and isinstance(port, int):
                self.addr = addr
                self.port = port
            else:
                raise Exception('createServer的参数类型有误')
        else:
            raise Exception('不支持client类型执行此方法')

    def startListen(self):
        # 先判断必填参数是否都填了
        if self._isServer():
            if self.serverProtocol is None:
                raise Exception('未设置protocol')
            if self.serverFactory is None:
                raise Exception('未设置factory')
            if self.addr is None or self.port is None:
                raise Exception('未设置createServer')
        else:
            raise Exception('未设置flag或不支持此方法')
        # 开启服务器，应该在子线程中一直运行
        # 调用父类的startListen方法，将数据传入
        factory = self.serverFactory
        factory.protocol = self.serverProtocol
        #if self.isAutoReconnect: # set auto-reconnection
            #factory.setProtocolOptions(autoPingInterval=5, autoPingTimeout=2)
        listenWS(factory)
        # reactor.listenTCP(self.port, factory)
        # reactor.run(installSignalHandlers=False)
        reactor.suggestThreadPoolSize(300)
        serverThread = threading.Thread(target=self._run, args=(reactor, factory), name='serverThread')
        serverThread.start()

    def _run(self, loop, server):
        try:

            loop.run(installSignalHandlers=False)
        except KeyboardInterrupt:
            pass
        finally:
            server.close()
            loop.stop()


    # 设置是否自动断线重连
    def setAutoReconnect(self, isAutoReconnect):
        if isinstance(isAutoReconnect, bool):
            self.isAutoReconnect = isAutoReconnect
        else:
            raise Exception('参数类型错误')

    def setServerProtocol(self, protocol):
        if self._isServer():
            self.serverProtocol = protocol
        else:
            raise Exception('Client类型不能调用此方法')

    def setServerFactory(self, factory):
        if self._isServer():
            self.serverFactory = factory
        else:
            raise Exception('Client类型不能调用此方法')

    def _isServer(self):
        if self.flag == self.Server:
            return True
        if self.flag == self.Client:
            return False
        else:
            raise Exception('未设置flag')