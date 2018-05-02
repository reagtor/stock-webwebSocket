# -*-coding:utf8-*-

from autobahn.twisted.websocket import WebSocketServerFactory
from autobahn.twisted.websocket import WebSocketServerProtocol
import copy
import json
import globaldata as RealTimeSnapshot


class BaseSipServerProtocol(WebSocketServerProtocol):

    def onConnect(self, request):
        print("客户端: {0}已连接。".format(request.peer))
        self.factory.addConnection(request.peer, self)
        self.onClientConnected(request.peer)

    def onOpen(self):
        # print(len(RealTimeSnapshot.Snapshot))
        try:
            if len(RealTimeSnapshot.Snapshot) > 0:

                for (k, v) in RealTimeSnapshot.Snapshot.items():
                    SnapDic = {}
                    strcode = k
                    if RealTimeSnapshot.DataSource == 'tushare':
                        mod = v.to_json()
                        SnapDic[strcode] = mod
                        result = json.dumps(SnapDic)
                        self.sendMessage(result.encode('utf-8'))

                    elif RealTimeSnapshot.DataSource == 'easyquotation':
                        mod = str(v)
                        SnapDic[strcode] = mod
                        result = json.dumps(SnapDic)
                        self.sendMessage(result.encode('utf-8'))

                    # if intTurn == RealTimeSnapshot.querymaxcodes or intTurn == len(RealTimeSnapshot.Snapshot):
                    #     sendlist = json.dumps(SnapDic)
                    #     print(sendlist)
                    #     self.sendMessage(sendlist)

        except Exception as exc:
            pass
            #logging.error(exc)

    def onMessage(self, payload, isBinary):
        # 做一些处理，peer mapping msg
        self.onMsgReceived(self.peer, payload, isBinary)

    def onClose(self, wasClean, code, reason):
        # print("WebSocket服务已关闭: {0}".format(reason))
        # remove connection from factory _connectionSets
        self.factory.removeConnection(self)
        self.onClientLostConnected(wasClean, code, reason)

    # client连接上来的时候回调
    def onClientConnected(self, peer):
        pass

    # client断开连接的时候回调
    def onClientLostConnected(self, wasClean, code, reason):
        pass

    # 收到Msg消息时回调
    def onMsgReceived(self, peer, data, isBinary):
        print('收到收据： {0}  来自： {1}'.format(data, peer))


class BaseSipServerFactory(WebSocketServerFactory):
    _connectionSets = dict()

    # save connection
    def addConnection(self, peer, connectedHandle):
        self._connectionSets.setdefault(peer, connectedHandle)

    # remove connection
    def removeConnection(self, connectedHandle):
        removePeer = None
        for k, v in self._connectionSets.items():
            if v == connectedHandle:
                removePeer = k
                break
        if removePeer is not None:
            self._connectionSets.pop(removePeer)
            print("WebSocket服务已关闭: {0}的连接。".format(removePeer))

    def getConnectionByPeer(self, peer):
        return self._connectionSets.get(peer)

    def getConnections(self):
        return copy.copy(self._connectionSets)

    # support sendMsg to client
    def sendMsg(self, peer, data):
        connectedHandle = self.getConnectionByPeer(peer)
        if connectedHandle is not None:
            if isinstance(data,bytes):
                connectedHandle.sendMessage(data, True)
            else:
                connectedHandle.sendMessage(data.encode('utf-8'))
        else:
            raise Exception('peer的连接不存在')
            # support sendMsg to client

    def broadcast(self, data):
        clients = self.getConnections()
        # print(len(clients))
        for client in clients:
            connectedHandle = self.getConnectionByPeer(client)
            if connectedHandle is not None:
                if isinstance(data, bytes):
                    connectedHandle.sendMessage(data, True)
                else:
                    connectedHandle.sendMessage(data.encode('utf-8'))
            else:
                raise Exception('peer的连接不存在')


