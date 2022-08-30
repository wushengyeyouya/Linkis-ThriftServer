# Linkis-ThriftServer

Linkis ThriftServer 服务，对外提供 ThriftServer 协议规范，用户可使用 Hive JDBC Driver 和 Hive ODBC Driver 访问 Linkis。

## 编译

```shell script
 cd Linkis-ThriftServer
 mvn -N install
 mvn clean package
```

## 安装

1. 解压

```shell script
 tar -zxvf linkis-thriftserver-remote-assembly-x.x.x.zip
```

2. 修改 `conf/linkis-thriftserver.properties` 配置文件

```shell script
 cd  linkis-thriftserver-remote-assembly
 vim conf/linkis-thriftserver.properties
```

```properties
 # Linkis Gateway 地址，需配置
 wds.linkis.gateway.url=http://127.0.0.1:9001/

 # LDAP，需配置
 wds.linkis.ldap.proxy.url=
 wds.linkis.ldap.proxy.baseDN=

 # Linkis ThriftServer 对外提供的端口号，默认为 10000
 linkis.thriftserver.port=10000

 # 后台提交的引擎，默认为 spark
 linkis.thriftserver.job.engine.type=spark
 linkis.thriftserver.job.engine.version=2.4.3
 # 提交给 Linkis 的 creator，默认为 Tableau
 job.creator=Tableau

 # 测试模式，默认不开启
 wds.linkis.test.mode=false

 linkis.thriftserver.webui.port=-1

 linkis.thriftserver.binary.cli.service.class=org.apache.hive.service.cli.thrift.LinkisCustomThriftBinaryCLIService

```

3. 启动

```shell script
 sh bin/start-linkis-thriftserver.sh
```