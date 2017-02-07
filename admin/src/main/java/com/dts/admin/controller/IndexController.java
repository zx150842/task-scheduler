package com.dts.admin.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * @author zhangxin
 */
@Controller
public class IndexController {
  @RequestMapping("/")
  public String index() {
    return "redirect:/job";
  }
}
