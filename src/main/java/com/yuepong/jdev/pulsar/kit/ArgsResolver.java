package com.yuepong.jdev.pulsar.kit;

import lombok.extern.slf4j.Slf4j;

/**
 * @ClassName: ArgsResolver
 * @Description:
 * @Author: apr
 * @Date: 2021/04/09 11:40:53
 **/
@Slf4j
public class ArgsResolver {
    public static String getParam(String args[], String argName, Object defaultValue){
        String param = defaultValue.toString();
        String target = "--".concat(argName).concat("=");
        if (args != null) for (String p : args) {
            if (p.startsWith(target)){
                param =  p.substring( 3+argName.length() ) ;
            }
        }

        log.info("Param\t"+argName+"\t\t{}", param );;
        return param;
    }
}
