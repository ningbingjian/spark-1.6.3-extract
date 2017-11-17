```abstract class RpcEndpointRef:
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
```

# Master如何启动?
```
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
```
# 如何实现Master的高可用?(standalone模式)
```
    0>spark master高可用是基于curator框架之上的实现
    1>Master.onStart方法
    2> 返回持久化引擎和leader选举代理
    persistenceEngine_ -> ZooKeeperPersistenceEngine
    leaderElectionAgent_ -> ZooKeeperLeaderElectionAgent
    3>在Master.scala onStart方法并没有看到是如何选举leader，仅在成员变量有
        private var state = RecoveryState.STANDBY
    4> 查看ZooKeeperLeaderElectionAgent的构造，有一个start()方法
    ZooKeeperLeaderElectionAgent(val masterInstance: LeaderElectable,
        conf: SparkConf) extends LeaderLatchListener with LeaderElectionAgent
    masterInstance -> Master
    LeaderLatchListener -> curator框架的LeaderLatchListener实现包含有下面两个抽象方法
    isLeader();//实现如果是leader的时候进行回调
    
    notLeader();//如果不是leader的时候进行回调
    
    5>LeaderElectionAgent是spark的抽象类,包含如下抽象方法
      val masterInstance: LeaderElectable
      def stop() {} // to avoid noops in implementations.
    6>  ZooKeeperLeaderElectionAgent.start方法会启动一个选举 
     private def start() {
        logInfo("Starting ZooKeeper LeaderElection agent")
        // CuratorFramework实例
        zk = SparkCuratorUtil.newClient(conf)
        //LeaderLatch 框架curator的类  选举关键类
        leaderLatch = new LeaderLatch(zk, WORKING_DIR)
        //添加监听器 
        leaderLatch.addListener(this)
        //启动选举
        leaderLatch.start()
      }
    7>ZooKeeperLeaderElectionAgent是LeaderLatchListener的实现 
    isLeader
    notLeader
    这两个方法的实现都差不多，主要是更新leader的状态，都会调用updateLeadershipStatus方法进行处理
     private def updateLeadershipStatus(isLeader: Boolean),注意，这两个抽象方法必须是同步的，保证每次判断和更新
     状态是原子执行
     updateLeadershipStatus有两个逻辑：
      - 如果是leader,修改状态并调用master.electedLeader [master实现了LeaderElectable，所以也实现了这个抽象方法]
      - 如果不是leader,修改状态并调用master.revokedLeadership[master实现了LeaderElectable，所以也实现了这个抽象方法]
    8>master.electedLeader 
     override def electedLeader() {
         self.send(ElectedLeader)
       }
       查看master接收到ElectedLeader的
       override def receive: PartialFunction[Any, Unit] = {
           case ElectedLeader => {
            第一步 判断zk是否有app,driver worker元数据信息
                如果没有修改状态是激活 ALIVE
                如果有，修改状态为RECOVERING
            第二步 如果状态是正在恢复 RECOVERING 开始启动恢复恢复完成后发送一个CompleteRecovery消息给自己   
            
           }
     9> 选举出新的master如何接管集群
        master需要恢复的数据包含三类
            app信息恢复
                0.新的master从zk读取app的信息
                1.master向所有app发送MasterChanged消息,并且app状态修改为UNKNOWN,想driver[实际是ClientApp.ClientEndpoint]发送MasterChanged
                2.driver端[实际是ClientApp.ClientEndpoint接收到masterChange的消息后修改masterRef
            driver信息恢复:
                0.从zk读取driver信息
                1.新的master有个成员变量drivers drivers += driver
            worker信息恢复:
                0.从zk恢复worker信息
                1.遍历worker重新在master端走一遍worker注册流程
                2.向所有的worker发送MasterChanged消息
                3.worker接收到MasterChanged消息,修改master地址
                发送worker当前的调度状态WorkerSchedulerStateResponse给master[包括workerId,executor信息,app信息,driver信息,executor状态 ]      
```

















		

	
	
	