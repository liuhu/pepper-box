package com.gslab.pepper.input;

import com.google.common.net.InetAddresses;
import org.apache.commons.lang3.StringUtils;
import sun.net.util.IPAddressUtil;

/**
 * Created by huangwei on 2/9/17.
 */
public class IpUtils {

    public static boolean isIpV4(String ip){
        return InetAddresses.isInetAddress(ip);
    }

    public static boolean isIpV4Pool(String ip, int mask){
        byte[] ipArray = IPAddressUtil.textToNumericFormatV4(ip);
        if(ipArray == null){
            return false;
        }
        return true;
    }

    public static Long[] ipV4ToRange(String ipAddr, int maskLen){
        int mask = 0xFFFFFFFF << (32 - maskLen);
        long ip = ipV4ToLong(ipAddr);

        long networkAddr = ip & mask;
        long boardCastAddr = networkAddr | (~mask);

        if (maskLen < 31) {
            // 掩码小于31位时 网络地址和广播地址不可用
            networkAddr = networkAddr + 1;
            boardCastAddr = boardCastAddr - 1;
        }
        return new Long[]{networkAddr, boardCastAddr};
    }

    public static void main(String[] args) {
        String ip = "10.32.0.200";
        int mask = 31;
        Long[] range = ipV4ToRange(ip, mask);
        System.out.println(range[0] + "," + range[1]);
        System.out.println("第一个可用: " + longToIpV4(range[0]));
        System.out.println("最后可用: " + longToIpV4(range[1]));
        System.out.println("可用地址数量:" + (range[1] - range[0] + 1));
    }

    public static long ipV4ToLong(String ip){
        String[] ips = ip.split("\\.");
        return  (Long.parseLong(ips[0]) << 24)
                | (Long.parseLong(ips[1]) << 16)
                | (Long.parseLong(ips[2]) << 8)
                | Long.parseLong(ips[3]);
    }

    public static String longToIpV4(long ipInt){
        return String.valueOf(((ipInt >> 24) & 0xff)) + '.' +
                ((ipInt >> 16) & 0xff) + '.' +
                ((ipInt >> 8) & 0xff) + '.' + (ipInt & 0xff);
    }

    public static boolean internalIp(String ip) {
        byte[] addr = IPAddressUtil.textToNumericFormatV4(ip);
        return internalIp(addr);
    }


    private static boolean internalIp(byte[] addr) {
        final byte b0 = addr[0];
        final byte b1 = addr[1];
        //10.x.x.x/8
        final byte SECTION_1 = 0x0A;
        //172.16.x.x/12
        final byte SECTION_2 = (byte) 0xAC;
        final byte SECTION_3 = (byte) 0x10;
        final byte SECTION_4 = (byte) 0x1F;
        //192.168.x.x/16
        final byte SECTION_5 = (byte) 0xC0;
        final byte SECTION_6 = (byte) 0xA8;
        switch (b0) {
            case SECTION_1:
                return true;
            case SECTION_2:
                if (b1 >= SECTION_3 && b1 <= SECTION_4) {
                    return true;
                }
            case SECTION_5:
                switch (b1) {
                    case SECTION_6:
                        return true;
                }
            default:
                return false;

        }
    }

    public static String getRemoteIpFromForward(String xforwardIp) {
        if (StringUtils.isBlank(xforwardIp)) {
            return "unknown";
        }
        int commaOffset = xforwardIp.indexOf(",");
        if (commaOffset < 0) {
            return xforwardIp;
        }
        return xforwardIp.substring(0, commaOffset);
    }
}
