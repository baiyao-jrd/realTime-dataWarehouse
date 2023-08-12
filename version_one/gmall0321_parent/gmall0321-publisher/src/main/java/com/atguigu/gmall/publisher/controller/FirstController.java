package com.atguigu.gmall.publisher.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Felix
 * @date 2022/9/17
 * @Controller 将类对象的创建交给Spring的IOC容器，如果类中的方法返回值的String的话，会进行页面的跳转
 *             如果不需要进行页面的跳转，只是做字符串的响应，可以在方法上加 @ResponseBody
 *             或者用@RestController替换@Controller
 *             @RestController = @Controller + @ResponseBody
 *
 * @RequestMapping("/first") 拦截客户端的请求，将请求交给标注的方法进行处理
 */
@RestController
public class FirstController {
    @RequestMapping("/first")
    public String first(@RequestParam("hehe") String username,
                        @RequestParam(value = "haha",defaultValue = "atguigu") String password){
        System.out.println(username + ":::" + password);
        return "success";
    }
}
