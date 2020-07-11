/*******************************************************************************
 * Copyright (C) 2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jms;

/**
 * Descriptions for the JMS operators
 */
public class JMSOpDescriptions {

	public static final String JMS_HEADER_PROPERTIES_DESC = ""						//$NON-NLS-1$
            + "Specifies the mapping between JMS Header property values and"		//$NON-NLS-1$
            + "Streams tuple attributes. The format of the mapping is: \\n\\n"		//$NON-NLS-1$
            + "\\\"propertyName1/streamsAttributeName1/typeSpecifier1\\\","			//$NON-NLS-1$
            + "\\\"propertyName2/streamsAttributeName2/typeSpecifier2\\\","			//$NON-NLS-1$
            + "\\\"...\\\" ;\\n\\n" 												//$NON-NLS-1$
            + "Leading and trailing spaces are trimmed from property and\\n\\n"		//$NON-NLS-1$
            + "attribute names. The properties can be of the following types:\\n"	//$NON-NLS-1$
            + "\\n"																	//$NON-NLS-1$
            + "----------------------------------------------------------------\\n"	//$NON-NLS-1$
            + "| **Type specifier** | **Java / Property type** | **SPL / Attribute type** |\\n"	//$NON-NLS-1$
            + "|--------------------------------------------------------------|\\n"	//$NON-NLS-1$
            + "| bool           | boolean              | boolean              |\\n"	//$NON-NLS-1$
            + "|--------------------------------------------------------------|\\n"	//$NON-NLS-1$
            + "| byte           | byte                 | int8                 |\\n" //$NON-NLS-1$
            + "|--------------------------------------------------------------|\\n"	//$NON-NLS-1$
            + "| short          | short                | int16                |\\n"	//$NON-NLS-1$
            + "|--------------------------------------------------------------|\\n"	//$NON-NLS-1$
            + "| int            | int                  | int32                |\\n"	//$NON-NLS-1$
            + "|--------------------------------------------------------------|\\n"	//$NON-NLS-1$
            + "| long           | long                 | int64                |\\n"	//$NON-NLS-1$
            + "|--------------------------------------------------------------|\\n"	//$NON-NLS-1$
            + "| float          | float                | float32              |\\n"	//$NON-NLS-1$
            + "|--------------------------------------------------------------|\\n"	//$NON-NLS-1$
            + "| double         | double               | float64              |\\n"	//$NON-NLS-1$
            + "|--------------------------------------------------------------|\\n"	//$NON-NLS-1$
            + "| string         | String               | rstring              |\\n"	//$NON-NLS-1$
            + "|--------------------------------------------------------------|\\n"	//$NON-NLS-1$
            + "| object         | Object               | ustring, uint8,      |\\n"	//$NON-NLS-1$
            + "|                |                      | uint16, uint32,      |\\n"	//$NON-NLS-1$
            + "|                |                      | uint64               |\\n"	//$NON-NLS-1$
            + "----------------------------------------------------------------\\n"	//$NON-NLS-1$
            + "\\n"																	//$NON-NLS-1$
            + "If property or attribute type do not match the specified type, "		//$NON-NLS-1$
            + "an error is logged during operator initialization."					//$NON-NLS-1$
            + "\\n\\n";																//$NON-NLS-1$

	
	public static final String JMS_HEADER_PROPERTIES_O_ATTR_NAME_DESC = ""			//$NON-NLS-1$
            + "Output attribute on output data stream to assign to the map\\n"		//$NON-NLS-1$
            + "containing all received properties. The specified attribute\\n"		//$NON-NLS-1$
            + "in output stream must be of type map<ustring,ustring>."				//$NON-NLS-1$
            + "\\n\\n";																//$NON-NLS-1$

	
	public static final String CLASS_LIBS = ""										//$NON-NLS-1$
            + "Allows the user to specify paths to JAR files that should be\\n"		//$NON-NLS-1$
            + "loaded into the operators classpath. The values of this parameter\\n"//$NON-NLS-1$
            + "may point to a specific JAR file, or to a directory. If pointing\\n"	//$NON-NLS-1$
            + "to a directory, the operator will load all files ending in .jar\\n"	//$NON-NLS-1$
            + "onto the classpath.\\n"												//$NON-NLS-1$
            + "If the parameter is set, only its values are used to identify\\n"	//$NON-NLS-1$
            + "class libraries. If the parameter is not set, the operator falls\\n"	//$NON-NLS-1$
            + "back to the old implementation referring to environment variables\\n"//$NON-NLS-1$
            + "STREAMS_MESSAGING_WMQ_HOME and STREAMS_MESSAGING_AMQ_HOME."			//$NON-NLS-1$
            + "\\n\\n";																//$NON-NLS-1$
	
}
