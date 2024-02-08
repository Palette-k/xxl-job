package com.xxl.job.executor.mvc.service;

import cn.iocoder.springboot.lab28.task.annotation.XxlRegister;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @author wuziqing
 * @version 1.0
 * @description: 测试 自动注册 xxl-job
 * @date 2024/2/5 15:19
 */
@Service
public class TestService {


    private static Logger logger = LoggerFactory.getLogger(TestService.class);

    @XxlJob(value = "testJob")
    @XxlRegister(cron = "0 0 0 * * ? *",
            author = "shark-chili",
            jobDesc = "测试job")
    public void testJob(){
        logger.info("testJob");
    }


    @XxlJob(value = "hello")
    @XxlRegister(cron = "0 0 0 * * ? *",
            triggerStatus = 1)
    public void hello(){
        logger.info("hello this is shark-chili");
    }


}

