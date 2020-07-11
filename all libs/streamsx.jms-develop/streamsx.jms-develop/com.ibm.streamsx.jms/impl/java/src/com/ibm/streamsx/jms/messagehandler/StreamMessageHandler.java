/*******************************************************************************
 * Copyright (C) 2013, 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jms.messagehandler;

import java.math.BigDecimal;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.Session;

import javax.jms.StreamMessage;

import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.Timestamp;
import com.ibm.streamsx.jms.types.MessageAction;
import com.ibm.streamsx.jms.types.NativeSchemaElement;
import com.ibm.streamsx.jms.types.NativeTypes;

//This class handles the JMS Stream message type 
public class StreamMessageHandler extends JMSMessageHandlerImpl {
	
	// constructor
	public StreamMessageHandler(List<NativeSchemaElement> nativeSchemaObjects) {
		super(nativeSchemaObjects);
	}

	// constructor
	public StreamMessageHandler(List<NativeSchemaElement> nativeSchemaObjects,
			Metric nTruncatedInserts) {
		super(nativeSchemaObjects, nTruncatedInserts);
	}

	// For JMSSink operator, convert the incoming tuple to a JMS StreamMessage
	public Message convertTupleToMessage(Tuple tuple, Session session)
			throws JMSException {
		// create a new StreamMessage
		StreamMessage streamMessage;
		synchronized (session) {
			streamMessage = session.createStreamMessage();
		}
		// variable to specify if any of the attributes in the message is
		// truncated
		boolean isTruncated = false;

		// get the attributes from the native schema
		for (NativeSchemaElement nsObj : nativeSchemaObjects) {
			// iterate through the native schema elements
			// extract the name, type and length

			final String		nsObjName 	= nsObj.getName();
			final NativeTypes	nsObjType 	= nsObj.getType();
			final int			nsObjLength = nsObj.getLength();
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
				// set the bytes in the message
				byte[] blobdata = new byte[(int) size];
				bl.getByteBuffer(0, (int) size).get(blobdata);
				streamMessage.writeBytes(blobdata);
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

						streamMessage.writeString(rdata);
					}
					// if the length of rdate is greater than the length
					// specified in native schema
					// set the isTruncated to true
					// truncate the String
					else if (size > nsObjLength) {
						isTruncated = true;
						String stringdata = rdata.substring(0, nsObjLength);
						streamMessage.writeString(stringdata);
					}
					break;
				// spl types decimal32, decimal64,decimal128, timestamp are
				// mapped to String.
				case DECIMAL32:
				case DECIMAL64:
				case DECIMAL128:
					streamMessage.writeString(tuple.getBigDecimal(nsObjName).toString());
					break;
				case TIMESTAMP:
					streamMessage.writeString((tuple.getTimestamp(nsObjName)
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
				streamMessage.writeByte(tuple.getByte(nsObjName));
				break;

			case Short:
				streamMessage.writeShort(tuple.getShort(nsObjName));
				break;

			case Int:
				streamMessage.writeInt(tuple.getInt(nsObjName));
				break;

			case Long:
				streamMessage.writeLong(tuple.getLong(nsObjName));
				break;

			case Float:
				streamMessage.writeFloat(tuple.getFloat(nsObjName));
				break;
			case Double:
				streamMessage.writeDouble(tuple.getDouble(nsObjName));
				break;
			case Boolean:
				streamMessage.writeBoolean(tuple.getBoolean(nsObjName));
				break;
			}
		}
		// if the isTruncated boolean is set, increment the metric
		// nTruncatedInserts
		if (isTruncated) {
			nTruncatedInserts.incrementValue(1);
		}
		return streamMessage;
	}

	// For JMSSource operator, convert the incoming JMS MapMessage to tuple
	public MessageAction convertMessageToTuple(Message message,
			OutputTuple tuple) throws JMSException {
		// We got a wrong message type so throw an error
		if (!(message instanceof StreamMessage)) {
			return MessageAction.DISCARD_MESSAGE_WRONG_TYPE;
		}
		StreamMessage streamMessage = (StreamMessage) message;
		// Iterate through the native schema attributes
		for (NativeSchemaElement nsObj : nativeSchemaObjects) {
			// Added the try catch block to catch the MessageEOFException
			// This exception must be thrown when an unexpected end of stream
			// has been reached when a StreamMessage is being read.
			try {
				// extract the name and type
				final String		nsObjName = nsObj.getName();
				final NativeTypes	nsObjType = nsObj.getType();
				// handle based on data-tye
				// extract the data from the message for each native schema
				// attrbute and set into the tuple
				switch (nsObjType) {
				case Byte:
					byte byteData = streamMessage.readByte();
					// we are interested in this current object only if it is
					// present in streams schema
					if (nsObj.getIsPresentInStreamSchema()) {
						tuple.setByte(nsObjName, byteData);
					}
					break;
				case Short:

					short shortData = streamMessage.readShort();
					// we are interested in this current object only if it is
					// present in streams schema
					if (nsObj.getIsPresentInStreamSchema()) {
						tuple.setShort(nsObjName, shortData);
					}
					break;
				case Int:

					int intData = streamMessage.readInt();
					// we are interested in this current object only if it is
					// present in streams schema
					if (nsObj.getIsPresentInStreamSchema()) {
						tuple.setInt(nsObjName, intData);
					}
					break;
				case Long:

					long longData = streamMessage.readLong();
					// we are interested in this current object only if it is
					// present in streams schema
					if (nsObj.getIsPresentInStreamSchema()) {
						tuple.setLong(nsObjName, longData);
					}
					break;
				case Float:

					float floatData = streamMessage.readFloat();
					// we are interested in this current object only if it is
					// present in streams schema
					if (nsObj.getIsPresentInStreamSchema()) {
						tuple.setFloat(nsObjName, floatData);
					}
					break;
				case Double:

					double doubleData = streamMessage.readDouble();
					// we are interested in this current object only if it is
					// present in streams schema
					if (nsObj.getIsPresentInStreamSchema()) {
						tuple.setDouble(nsObjName, doubleData);
					}
					break;
				case Boolean:

					boolean booleanData = streamMessage.readBoolean();
					// we are interested in this current object only if it is
					// present in streams schema
					if (nsObj.getIsPresentInStreamSchema()) {
						tuple.setBoolean(nsObjName, booleanData);
					}
					break;
				case String:

					switch (tuple.getStreamSchema().getAttribute(nsObjName).getType().getMetaType()) {
					case RSTRING:
					case USTRING:
						String stringData = streamMessage.readString();
						// we are interested in this current object only if it is
						// present in streams schema
						if (nsObj.getIsPresentInStreamSchema()) {
							tuple.setString(nsObjName, stringData);
						}
						break;
					case DECIMAL32:
					case DECIMAL64:
					case DECIMAL128: {
						BigDecimal bigDecValue = new BigDecimal(
								streamMessage.readString());
						// we are interested in this current object only if it is
						// present in streams schema
						if (nsObj.getIsPresentInStreamSchema()) {
							tuple.setBigDecimal(nsObjName, bigDecValue);
						}
					}
						break;
					case TIMESTAMP: {
						BigDecimal bigDecValue = new BigDecimal(
								streamMessage.readString());
						// we are interested in this current object only if it is
						// present in streams schema
						if (nsObj.getIsPresentInStreamSchema()) {
							tuple.setTimestamp(nsObjName,
									Timestamp.getTimestamp(bigDecValue));
						}
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
			} catch (MessageEOFException meofEx) {
				return MessageAction.DISCARD_MESSAGE_EOF_REACHED;
			} catch (MessageNotReadableException mnrEx) {
				return MessageAction.DISCARD_MESSAGE_UNREADABLE;
			} catch (MessageFormatException mfEx) {
				return MessageAction.DISCARD_MESSAGE_MESSAGE_FORMAT_ERROR;
			}

		}
		
		// Message was successfully read
		return MessageAction.SUCCESSFUL_MESSAGE;
	}
}
