package cn.ngt.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Created on 2021-06-05 16:51.
 *
 * @author ngt
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}

/*
Lombok是一种Java™实用工具，可用来帮助开发人员消除Java的冗长代码，尤其是对于简单的Java对象（POJO）。它通过注释实现这一目的
使用准备
1.IntelliJ安装Lombok 插件
2.导入依赖

https://juejin.cn/post/6844903557016076302
@Data：注解在类上，相当于同时使用了@ToString、@EqualsAndHashCode、@Getter、@Setter和@RequiredArgsConstrutor这些注解，对于POJO类十分有用
 */