package com.github.sroigmas.avro.reflection;

import org.apache.avro.reflect.Nullable;

public class ReflectedCustomer {

  private String firstName;
  private String lastName;
  @Nullable
  private String nickName;

  public ReflectedCustomer() {
  }

  public ReflectedCustomer(String firstName, String lastName, String nickName) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.nickName = nickName;
  }

  /*public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public String getNickName() {
    return nickName;
  }

  public void setNickName(String nickName) {
    this.nickName = nickName;
  }*/

  public String fullName() {
    return this.firstName + " " + this.lastName + " " + this.nickName;
  }
}
