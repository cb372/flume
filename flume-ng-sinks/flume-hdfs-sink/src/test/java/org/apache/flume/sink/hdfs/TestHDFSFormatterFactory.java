/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.hdfs;

import static org.junit.Assert.assertTrue;

import org.apache.flume.Context;
import org.apache.flume.sink.FlumeFormatter;
import org.junit.Test;

public class TestHDFSFormatterFactory {

  @Test
  public void getTextFormatter() {
    FlumeFormatter formatter = HDFSFormatterFactory.getFormatter("Text", new Context());

    assertTrue(formatter != null);   
    assertTrue(formatter.getClass().getName(), 
      formatter instanceof HDFSTextFormatter);   
  }  

  @Test
  public void getWritableFormatter() {
    FlumeFormatter formatter = HDFSFormatterFactory.getFormatter("Writable", new Context());

    assertTrue(formatter != null);   
    assertTrue(formatter.getClass().getName(), 
      formatter instanceof HDFSWritableFormatter);   
  }  

  @Test
  public void getCustomFormatter() {
    FlumeFormatter formatter = HDFSFormatterFactory.getFormatter(
      "org.apache.flume.sink.hdfs.MyCustomFormatter$Builder", new Context());

    assertTrue(formatter != null);   
    assertTrue(formatter.getClass().getName(), 
      formatter instanceof MyCustomFormatter);   
  }  

}
