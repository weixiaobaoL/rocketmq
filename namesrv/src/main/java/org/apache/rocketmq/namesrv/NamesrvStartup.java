/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

/*
* 这个就是最为关键的name server 的启动类。
* */
public class NamesrvStartup {

    /**
     * 最开始看源码的时候不需要把所有的类都了解清楚，下面这三个类根据名字猜测应该是和日志，属性配置，命令行有关的。
     * 和核心逻辑没有多少关系，所以前期看源码的时候不需要重点关心相关的方法
     */
    private static InternalLogger log;
    private static Properties properties = null;
    private static CommandLine commandLine = null;

    public static void main(String[] args) {
        main0(args);
    }

    /**
     * 初步看一眼下面的代码，可以知道核心的组件是NamesrvController这个对象
     */
    public static NamesrvController main0(String[] args) {

        try {
            /*
            这行代码很明显，就是在创建一个NamesrvController类，这个类似乎是NameServer中的一个核心组件。
            那么大家觉得这个类可能会是用来干什么的呢？
                我们可以大胆的推测一下，NameServer启动之后，是不是需要接受Broker的请求？因为Broker都要把自己注册到NameServer上去。
                然后Producer这些客户端是不是也要从NameServer拉取元数据？因为他们需要知道一个Topic的MessageQueue都在哪些Broker上。
                所以我们完全可以猜想一下，NamesrvController这个组件，很可能就是NameServer中专门用来接受Broker和客户端的网络请求的一个组件！
                因为平时我们写Java Web系统的时候，大家都喜欢用Spring MVC框架，在Spring MVC框架中，用于接受HTTP请求的，就是Controller组件！
             */
            NamesrvController controller = createNamesrvController(args);
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {
        //region 解析一下我们传递进去的一些命令行参数
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }
        //endregion

        //region 非常核心的两个NameServer的配置类: NamesrvConfig、NettyServerConfig
        //NameServer自身运行的一些配置参数
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        //用于接收网络请求的Netty服务器的配置参数，在这里也能明确感觉到，NameServer对外接收Broker和客户端的网络请求的时候，底层应该是基于Netty实现的网络服务器
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        //这里可以看见为什么name service 的默认参数是9876了
        nettyServerConfig.setListenPort(9876);
        //endregion

        //region 根据命令行启动的时候是否带了 -c 这个选项: 这个选项的意思就是读取一个配置文件的地址。并且读取这个配置文件的信息到properties里面去
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                //把文件的信息放入到properties里面去
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);
                //通过工具吧文件里面的配置放入到两个核心的配置类里面去
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);

                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }
        //endregion

        //region 根据命令行启动的时候是否带了 -p 这个选项: 这个选项的意思就是print 把NameService的所有配置信息打印出来。然后退出，也就是不会运行name server
        if (commandLine.hasOption('p')) {
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            System.exit(0);
        }
        //endregion

        //region 就是把在使用mqnamesrv命令行中带上的配置选项，都读取出来，然后覆盖到NameSrvConfig里面去
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
        //endregion


        //region 这个是个安全检查，判断我们的环境变量里面是否有ROCKETMQ_HOME这个值，如果没有就提示我们需要设置环境变量，然后退出。
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        //endregion

        //region 这个逻辑根据一些类名可以知道是和日志、以及日志配置相关的，所以不会影响到核心的逻辑，在研究rocketmq的核心逻辑的时候可以选择跳过
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        //endregion

        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        // remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                controller.shutdown();
                return null;
            }
        }));

        controller.start();

        return controller;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
