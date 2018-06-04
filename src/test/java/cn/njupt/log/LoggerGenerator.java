package cn.njupt.log;

import org.apache.log4j.Logger;

/**
 * 模拟日志产生并直接输出到Flume
 */
public class LoggerGenerator {
    public static final Logger LOGGER = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws Exception {
        int index = 0;

        while (true) {
            Thread.sleep(1000);

            LOGGER.info("current value : " + index);

            index++;
        }
    }
}
