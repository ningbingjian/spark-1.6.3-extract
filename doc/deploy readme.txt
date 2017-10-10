abstract class RpcEndpointRef:
	def address: RpcAddress
	def name: String
	def send(message: Any): Unit
	def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]
	def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)
	def askWithRetry[T: ClassTag](message: Any): T = askWithRetry(message, defaultAskTimeout)
trait RpcEndpoint:
	val rpcEnv: RpcEnv
	final def self: RpcEndpointRef 
	def receive: PartialFunction[Any, Unit]
	def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
	def onError(cause: Throwable): Unit
	def onConnected(remoteAddress: RpcAddress): Unit 
	def onDisconnected(remoteAddress: RpcAddress): Unit
	def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit
	def onStart(): Unit
	def onStop(): Unit
	final def stop(): Unit
	
abstract class RpcEnv:
	private[spark] val defaultLookupTimeout
	private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef
	def address: RpcAddress
	def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef
	def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]
	def setupEndpointRefByURI(uri: String): RpcEndpointRef 
	def setupEndpointRef(
	systemName: String, address: RpcAddress, endpointName: String): RpcEndpointRef
	def stop(endpoint: RpcEndpointRef): Unit
	def shutdown(): Unit
	def awaitTermination(): Unit
	def uriOf(systemName: String, address: RpcAddress, endpointName: String): String
	def deserialize[T](deserializationAction: () => T): T
	def fileServer: RpcEnvFileServer
	def openChannel(uri: String): ReadableByteChannel

	

	
	
trait RpcCallContext:
  def reply(response: Any): Unit
  def sendFailure(e: Throwable): Unit
  def senderAddress: RpcAddress
abstract class NettyRpcCallContext extends RpcCallContext 
  protected def send(message: Any): Unit
class LocalNettyRpcCallContext extends NettyRpcCallContext(senderAddress):
class RemoteNettyRpcCallContext extends NettyRpcCallContext(senderAddress):


Master如何启动?
1.Master.main  ->Master.startRpcEnvAndEndpoint ->  NettyRpcEnv.awaitTermination() -> masterEndpoint.askWithRetry

2.Master.startRpcEnvAndEndpoint
RpcEnv.create -> rpcEnv.setupEndpoint -> masterEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)
3. RpcEnv.create
NettyRpcEnvFactory.create
4.NettyRpcEnvFactory.create
new NettyRpcEnv -> nettyEnv.startServer
5.	nettyEnv.startServer
transportContext.createServer -> dispatcher.registerRpcEndpoint[endpoint-verifier]
启动了一个端口，所有发过来的请求根据endpoint的来通讯
6.rpcEnv.setupEndpoint
Dispatcher.registerRpcEndpoint -> 注册了masterEndpoint 
7.当 registerRpcEndpoint的时候会给endpoint发送一个OnStart消息
所以会调用Master.onStart
8.Master.OnStart
	1>启动webUi
	2>启动restServer
	3>启动masterMetricsSystem
	4>启动masterMetricsSystem
	5>配置恢复选项 zookeeper可完成高可用
9.	

















		

	
	
	