/*******************************************************************************
 * Copyright (C) 2014,2019 International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jms.messagehandler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.ValueFactory;
import com.ibm.streams.operator.types.XML;
import com.ibm.streamsx.jms.types.MessageAction;
import com.ibm.streamsx.jms.types.NativeSchemaElement;

/**
 * 
 * Message handler for text message class.
 * 
 * For text message class, the following restrictions applies:
 * * Text messages must be encoded in UTF-8.  For support of other encoding, use bytes message class.
 * * When the text message class is specified in the connection document, the native
 * schema must contain an attribute of String.  
 * * When the text message class is specified in the connection document, the input schema
 * for JMSSource must contain an attribute of type rstring, ustring or xml.
 * When the text message class is specified in the connection document, the output schema
 * for JMSSink must contain an attribute of type rstring, ustring or xml.
 *
 */
public class TextMessageHandler extends JMSMessageHandlerImpl {

	private String	attributeNameNS;
	private int		msgLengthNS;
	
	

	public TextMessageHandler(List<NativeSchemaElement> nativeSchemaObjects) {
		super(nativeSchemaObjects);
		
		// This should not happen as length of native schema has already been checked
		if (nativeSchemaObjects.size() != 1) return;
		
		NativeSchemaElement nse = nativeSchemaObjects.get(0);
		attributeNameNS	= nse.getName();
		msgLengthNS		= nse.getLength();
	}

	
	
	@Override
	public Message convertTupleToMessage(Tuple tuple, Session session) throws JMSException,
			UnsupportedEncodingException, ParserConfigurationException, TransformerException {

		TextMessage textMessage;
		synchronized (session) {
			textMessage = session.createTextMessage();
		}
		
		StreamSchema streamSchema = tuple.getStreamSchema();
		int attributeIdx = determineAttributeIndex(streamSchema);
		
		MetaType attrType = streamSchema.getAttribute(attributeIdx).getType().getMetaType();

		String msgText = ""; //$NON-NLS-1$

		if (attrType == MetaType.RSTRING || attrType == MetaType.USTRING) {
			// use getObject to avoid copying of the tuple
			Object tupleVal = tuple.getObject(attributeIdx);
			if (tupleVal instanceof RString) {
				msgText = ((RString) tupleVal).getString();
			} else if (tupleVal instanceof String) {
				msgText = (String) tupleVal;
			}
		} else if (attrType == MetaType.XML) {
			XML xmlValue = tuple.getXML(attributeIdx);
			msgText = xmlValue.toString();
		}

		// make sure message length is < the length specified in native schema
		if (msgLengthNS > 0 && msgText.length() > msgLengthNS) {
			msgText = msgText.substring(0, msgLengthNS);
			nTruncatedInserts.increment();
		}
		textMessage.setText(msgText);

		return textMessage;
	}

	@Override
	public MessageAction convertMessageToTuple(Message message, OutputTuple tuple) throws JMSException, 
			UnsupportedEncodingException {

		TextMessage textMessage = (TextMessage) message;

		// make sure message length is < the length specified in native schema
		String tupleStr = textMessage.getText();
		if (msgLengthNS > 0 && tupleStr.length() > msgLengthNS) {
			tupleStr = tupleStr.substring(0, msgLengthNS);
		}

		StreamSchema streamSchema = tuple.getStreamSchema();
		int attributeIdx = determineAttributeIndex(streamSchema);
		
		MetaType attrType = tuple.getStreamSchema().getAttribute(attributeIdx).getType().getMetaType();

		if (attrType == MetaType.RSTRING || attrType == MetaType.USTRING) {
			tuple.setString(attributeIdx, tupleStr);
		} else if (attrType == MetaType.XML) {
			ByteArrayInputStream inputStream = new ByteArrayInputStream(tupleStr.getBytes());
			try {
				XML xmlValue = ValueFactory.newXML(inputStream);
				tuple.setXML(attributeIdx, xmlValue);
			} catch (IOException e) {		
				// unable to convert incoming string to xml value
				// discard message and continue
				return MessageAction.DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR;
			}
		}

		return MessageAction.SUCCESSFUL_MESSAGE;
	}
	

	private int determineAttributeIndex(StreamSchema streamSchema) {
		int idx = 0;
		
		if(streamSchema.getAttributeCount() > 1)
			idx = streamSchema.getAttributeIndex(attributeNameNS);
		
		return idx;
	}

}
