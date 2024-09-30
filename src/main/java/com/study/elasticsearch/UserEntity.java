package com.study.elasticsearch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * @author ChenYu ren
 * @date 2024/8/7
 */

@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UserEntity {

    private Integer id;

    private String name;

    private int age;

    private String sex;
}
