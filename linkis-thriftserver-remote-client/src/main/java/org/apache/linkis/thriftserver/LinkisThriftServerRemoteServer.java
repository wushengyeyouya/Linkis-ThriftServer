package org.apache.linkis.thriftserver;

import org.apache.linkis.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author enjoyyin
 * @date 2022-08-19
 * @since 0.5.0
 */
public class LinkisThriftServerRemoteServer {

    private static final Logger LOG = LoggerFactory.getLogger(LinkisThriftServerRemoteServer.class);

    public static void main(String[] args) {
        String userName = System.getProperty("user.name");
        String hostName = Utils.getComputerName();
        System.setProperty("hostName", hostName);
        System.setProperty("userName", userName);
        System.setProperty("wds.linkis.configuration", "linkis-thriftserver.properties");
        if(args == null) {
            args = new String[]{};
        }
        LOG.info("Ready to start {} with args: {}.", hostName, Arrays.asList(args));
        LinkisThriftServer2$.MODULE$.start();
    }
}
