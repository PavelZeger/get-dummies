package com.zeger.util

import org.slf4j.{Logger, LoggerFactory}

/**
 * @author Pavel Zeger
 * @since 28/01/2021
 * @implNote get-dummies
 */
trait LoggerSetup {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

}