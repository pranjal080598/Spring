/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jms.messagehandler;

import java.math.BigDecimal;
import java.util.List;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.Session;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.Timestamp;
import com.ibm.streamsx.jms.types.MessageAction;
import com.ibm.streamsx.jms.types.NativeSchemaElement;
import com.ibm.streamsx.jms.types.NativeTypes;


public class MapMessageHandler extends JMSMessageHandlerImpl {
	
	public MapMessageHandler(List<NativeSchemaElement> nativeSchemaObjects) {
		super(nativeSchemaObjects);
	}

	
	public MapMessageHandler(List<NativeSchemaElement> nativeSchemaObjects, Metric nTruncatedInserts) {
		super(nativeSchemaObjects, nTruncatedInserts);
	}

	
	// For JMSSink operator, convert the incoming tuple to a JMS MapMessage
	public Message convertTupleToMessage(Tuple tuple, Session session) throws JMSException {

		MapMessage mapMessage;
		synchronized (session) {
			mapMessage = (MapMessage) session.createMapMessage();
		}
		// variable to specify if any of the attributes in the message is
		// truncated
		boolean isTruncated = false;

		// get the attributes from the native schema
		for (NativeSchemaElement nsObj : nativeSchemaObjects) {
			
			// iterate through the native schema elements
			// extract the name, type and length
			final String		nsObjName	= nsObj.getName();
			final NativeTypes	nsObjType	= nsObj.getType();
			final int			nsObjLength	= nsObj.getLength();
			
			// handle based on the data-type
			switch (nsObjType) {

			// For all cases, IllegalArgumentException and NPE(for setBytes) is
			// not caught since name is always verified and is not null or not
			// empty string.
			case Bytes: {
				// extract the blob from the tuple
				// get its size
				Blob bl = tuple.getBlob(nsObjName);
				long size = bl.getLength();

				// check for length in native schema
				// if the length of the blob is greater than the length
				// specified in native schema
				// set the isTruncated to true
				// truncate the blob
				if (size > nsObjLength && nsObjLength != LENGTH_ABSENT_IN_NATIVE_SCHEMA) {
					isTruncated = true;
					size = nsObjLength;
				}
				byte[] blobdata = new byte[(int) size];
				bl.getByteBuffer(0, (int) size).get(blobdata);
				// Since name is always verified and never null or empty string,
				// we dont need to catch NPE

				// set the bytes into the messaage
				mapMessage.setBytes(nsObjName, blobdata);
			}
				break;
			case String: {
				switch (tuple.getStreamSchema().getAttribute(nsObjName).getType().getMetaType()) {
				case RSTRING:
				case USTRING:
					// extract the String
					// get its length
					String rdata = tuple.getString(nsObjName);
					int size = rdata.length();
					// If no length was specified in native schema or
					// if the length of the String rdata is less than the length
					// specified in native schema
					if (nsObjLength == LENGTH_ABSENT_IN_NATIVE_SCHEMA
							|| size <= nsObjLength) {
						mapMessage.setString(nsObjName, rdata);
					}
					// if the length of rdate is greater than the length
					// specified in native schema
					// set the isTruncated to true
					// truncate the String
					else if (size > nsObjLength) {
						isTruncated = true;
						String stringdata = rdata.substring(0, nsObjLength);
						mapMessage.setString(nsObjName, stringdata);
					}

					break;
				// spl types decimal32, decimal64,decimal128, timestamp are
				// mapped to String.
				case DECIMAL32:
				case DECIMAL64:
				case DECIMAL128:
					mapMessage.setString(nsObjName, tuple.getBigDecimal(nsObjName)
							.toString());
					break;
				case TIMESTAMP:
					mapMessage.setString(nsObjName, (tuple.getTimestamp(nsObjName)
							.getTimeAsSeconds()).toString());
					break;

				case BLIST:
				case BLOB:
				case BMAP:
				case BOOLEAN:
				case BSET:
				case BSTRING:
				case COMPLEX32:
				case COMPLEX64:
				case ENUM:
				case FLOAT32:
				case FLOAT64:
				case INT8:
				case INT16:
				case INT32:
				case INT64:
				case LIST:
				case MAP:
				case OPTIONAL:
				case SET:
				case TUPLE:
				case UINT8:
				case UINT16:
				case UINT32:
				case UINT64:
				case XML:
					// DO NOTHING
					break;
				}
			}
				break;
			case Byte:
				mapMessage.setByte(nsObjName, tuple.getByte(nsObjName));
				break;
			case Short:
				mapMessage.setShort(nsObjName, tuple.getShort(nsObjName));
				break;
			case Int:
				mapMessage.setInt(nsObjName, tuple.getInt(nsObjName));
				break;
			case Long:
				mapMessage.setLong(nsObjName, tuple.getLong(nsObjName));
				break;
			case Float:
				mapMessage.setFloat(nsObjName, tuple.getFloat(nsObjName));
				break;
			case Double:
				mapMessage.setDouble(nsObjName, tuple.getDouble(nsObjName));
				break;
			case Boolean:
				mapMessage.setBoolean(nsObjName, tuple.getBoolean(nsObjName));
				break;
			}
		}
		// if the isTruncated boolean is set, increment the metric
		// nTruncatedInserts
		if (isTruncated) {
			nTruncatedInserts.incrementValue(1);
		}
		// return the message
		return mapMessage;
	}

	// For JMSSource operator, convert the incoming JMS MapMessage to tuple
	public MessageAction convertMessageToTuple(Message message,
			OutputTuple tuple) throws JMSException {
		if (!(message instanceof MapMessage)) {
			// We got a wrong message type so throw an error
			return MessageAction.DISCARD_MESSAGE_WRONG_TYPE;
		}
		else {
			MapMessage mapMessage = (MapMessage) message;
			
			// Iterate through the native schema attributes
			for (NativeSchemaElement nsObj : nativeSchemaObjects) {

				try {
					// extract the name, type and length
					final String		nsObjName = nsObj.getName();
					final NativeTypes	nsObjType = nsObj.getType();
					// handle based on data-type
					// extract the data from the message for each native schema
					// attribute and set into the tuple
					// we are interested in the current object only if it is
					// present in streams schema
					if (nsObj.getIsPresentInStreamSchema())
						switch (nsObjType) {
						case Byte:
							tuple.setByte(nsObjName, mapMessage.getByte(nsObjName));
							break;
						case Short:
							tuple.setShort(nsObjName, mapMessage.getShort(nsObjName));
							break;
						case Int:
							tuple.setInt(nsObjName, mapMessage.getInt(nsObjName));
							break;
						case Long:
							tuple.setLong(nsObjName, mapMessage.getLong(nsObjName));
							break;
						case Float:
							tuple.setFloat(nsObjName, mapMessage.getFloat(nsObjName));
							break;
						case Double:
							tuple.setDouble(nsObjName, mapMessage.getDouble(nsObjName));
							break;
						case Boolean:
							tuple.setBoolean(nsObjName, mapMessage.getBoolean(nsObjName));
							break;
						case String:
							switch (tuple.getStreamSchema().getAttribute(nsObjName).getType().getMetaType()) {
							case RSTRING:
							case USTRING:
								tuple.setString(nsObjName,
										mapMessage.getString(nsObjName));
								break;
							case DECIMAL32:
							case DECIMAL64:
							case DECIMAL128: {

								BigDecimal bigDecValue = new BigDecimal(
										mapMessage.getString(nsObjName));
								tuple.setBigDecimal(nsObjName, bigDecValue);
							}
								break;
							case TIMESTAMP: {
								BigDecimal bigDecValue = new BigDecimal(
										mapMessage.getString(nsObjName));
								tuple.setTimestamp(nsObjName,
										Timestamp.getTimestamp(bigDecValue));
							}
								break;

							case BLIST:
							case BLOB:
							case BMAP:
							case BOOLEAN:
							case BSET:
							case BSTRING:
							case COMPLEX32:
							case COMPLEX64:
							case ENUM:
							case FLOAT32:
							case FLOAT64:
							case INT8:
							case INT16:
							case INT32:
							case INT64:
							case LIST:
							case MAP:
							case OPTIONAL:
							case SET:
							case TUPLE:
							case UINT8:
							case UINT16:
							case UINT32:
							case UINT64:
							case XML:
								// DO NOTHING
								break;
							}
							break;

						case Bytes:
							// DO NOTHING
							break;
						}
				} catch (MessageFormatException mfEx) {
					// if MessageFormatException is thrown
					return MessageAction.DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR;
				}

			}
			// Messsage was successfully read
			return MessageAction.SUCCESSFUL_MESSAGE;
		}
	}
}
