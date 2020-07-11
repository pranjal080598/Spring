/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jms.messagehandler;

import java.util.List;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streamsx.jms.types.NativeSchemaElement;

//abstract base class for all JMS message classes 
public abstract class JMSMessageHandlerImpl implements JMSMessageHandler {

	// This holds all the attributes coming in the native schema
	// oject in connections.xml
	protected final List<NativeSchemaElement> nativeSchemaObjects;
	// This metric indicates the number of tuples that had truncated
	// attributes while converting to a message for JMSSink
	Metric nTruncatedInserts;
	// Identifier to indicate if length is missing from an
	// attribute in native schema
	static final int LENGTH_ABSENT_IN_NATIVE_SCHEMA = -999;

	// Constructor to initialize the nativeSchemaObjects

	JMSMessageHandlerImpl(List<NativeSchemaElement> nsa) {
		this.nativeSchemaObjects = nsa;
	}

	// Constructor to initialize the nativeSchemaObjects and nTruncatedInserts
	JMSMessageHandlerImpl(List<NativeSchemaElement> nsa, Metric nTruncatedInserts) {
		this.nativeSchemaObjects = nsa;
		this.nTruncatedInserts = nTruncatedInserts;
	}
}