package com.ruisdata.starbucks.entity;

import lombok.Data;

/**
 * @Author guihao.qing
 * @description
 * @since 2019-12-11
 **/
@Data
public class ConfigEntity {
    private Integer id;
    private Integer code;
    private String name;
    private String startDay;
    private String endDay;
    private String comment;
}
