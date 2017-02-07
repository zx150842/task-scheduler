package com.dts.executor.example.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zhangxin
 */
@RestController
public class ExampleController {
  @RequestMapping("/ping")
  public String ping() {
    return "OK";
  }
}
