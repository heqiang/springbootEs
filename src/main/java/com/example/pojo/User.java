package com.example.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.naming.ldap.PagedResultsControl;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
    private  int  id;
    private  String name;
    private int age;
}
