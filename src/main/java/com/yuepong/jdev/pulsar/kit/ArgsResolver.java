package com.yuepong.jdev.pulsar.kit;

/**
 * @ClassName: ArgsResolver
 * @Description:
 * @Author: apr
 * @Date: 2021/04/09 11:40:53
 **/
public class ArgsResolver {
    public static String getParam(String args[], String argName, Object defaultValue){
        String target = "--".concat(argName).concat("=");
        if (args != null) for (String p : args) {
            if (p.startsWith(target)){
                 return p.substring( 3+argName.length() );
            }
        }

        return defaultValue.toString();
    }
}
