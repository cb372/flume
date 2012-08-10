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

import com.google.common.base.Preconditions;
import org.apache.flume.sink.FlumeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSFormatterFactory {

  private static final Logger logger =
      LoggerFactory.getLogger(HDFSFormatterFactory.class);

  static FlumeFormatter getFormatter(String formatType) {

    Preconditions.checkNotNull(formatType,
        "format type must not be null");

    // try to find formatter class in enum of known formatters
    HDFSFormatterType type;
    try {
      type = HDFSFormatterType.valueOf(formatType);
    } catch (IllegalArgumentException e) {
      logger.debug("Not in enum, loading formatter class: {}", formatType);
      type = HDFSFormatterType.Other;
    }
    Class<? extends FlumeFormatter> formatterClass =
        type.getFormatterClass();

    // handle the case where they have specified their own formatter in the config
    if (formatterClass == null) {
      try {
        Class c = Class.forName(formatType);
        if (c != null && FlumeFormatter.class.isAssignableFrom(c)) {
          formatterClass = (Class<? extends FlumeFormatter>) c;
        } else {
          logger.error("Unable to instantiate FlumeFormatter from {}", formatType);
          return null;
        }
      } catch (ClassNotFoundException ex) {
        logger.error("Class not found: " + formatType, ex);
        return null;
      }
    }

    // instantiate the formatter
    FlumeFormatter formatter;
    try {
      formatter = formatterClass.newInstance();
    } catch (InstantiationException ex) {
      logger.error("Cannot instantiate formatter: " + formatType, ex);
      return null;
    } catch (IllegalAccessException ex) {
      logger.error("Cannot instantiate formatter: " + formatType, ex);
      return null;
    }

    return formatter;
  }

}
