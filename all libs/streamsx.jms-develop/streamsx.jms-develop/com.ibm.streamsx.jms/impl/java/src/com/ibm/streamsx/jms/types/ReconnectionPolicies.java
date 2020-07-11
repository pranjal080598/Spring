/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jms.types;

/* Enum to define the reconnection policies supported for both the operators in case
 * of both initial or transient connection failures.

 * The valid values are:
 * BoundedRetry=Bounded number of re-tries
 * NoRetry=No Retry
 * InfiniteRetry= Infinite number of retry
 * 
 */
public enum ReconnectionPolicies {
	BoundedRetry, NoRetry, InfiniteRetry;
}
