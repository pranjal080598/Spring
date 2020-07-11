/*******************************************************************************
 * Copyright (C) 2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jms.helper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streamsx.jms.JMSOpConstants;
import com.ibm.streamsx.jms.exceptions.JmsHeaderPropertiesConfigurationException;
import com.ibm.streamsx.jms.i18n.Messages;
import com.ibm.streamsx.jms.types.PropertyType;

public class JmsHeaderHelper {

	/**
	 * @param context
	 * @param streamSchema
	 * @param patTriplets
	 * @param logger
	 * @param tracer
	 */
	public static void prepareJmsHeaderPropertiesAccess(OperatorContext context,
														StreamSchema streamSchema,
														List<PropertyAttributeType> patTriplets,
														Logger logger,
														Logger tracer)
	{
		List<String>	usedPropertyNames								= new ArrayList<>();
		List<String>	usedAttributeNames								= new ArrayList<>();
    	StringBuffer	configurationErrorMessages						= new StringBuffer();
    	boolean			throwJmsHeaderPropertiesConfigurationException	= false;
    	
    	List<String> jmsHeaderProperties = context.getParameterValues(JMSOpConstants.PARAM_JMS_HEADER_PROPS);
    	for(String propAttrTypeTriplet : jmsHeaderProperties) {
    		String trimmedTriplet = propAttrTypeTriplet.trim();
    		
    		// Tokenize triplet string into property/attribute triplets around the slashes
    		List<String> propAttrType = Arrays.asList(trimmedTriplet.split("/"));	//$NON-NLS-1$
    		
    		// We expect to have three values
    		if(propAttrType.size() != 3) {
    			String errMsg = Messages.getString("PROPERTY_ATTRIBUTE_TYPE_SPLIT_FAILED", new Object[] { propAttrType.toString() });	//$NON-NLS-1$
    			logger.log(LogLevel.ERROR, errMsg);
    			configurationErrorMessages.append(errMsg);
    			
    			// If we do not have three values the next checks
    			// will fail just because of that, so we continue
    			// right away
    			continue;
    		}
        		
        		
    		boolean erroneousConfiguration	= false;
    		String	propertyName			= propAttrType.get(0);
    		String	attributeName			= propAttrType.get(1);
    		String	typeSpec				= propAttrType.get(2);

    			
    		// A referenced attribute must be part of the attached stream schema
        	int attributeIdx = streamSchema.getAttributeIndex(attributeName);
        	if(attributeIdx == -1) {
        		String errMsg = Messages.getString("PROPERTY_ATTRIBUTE_TYPE_UNKNOWN_ATTRIBUTE", new Object[] { attributeName });	//$NON-NLS-1$
        		logger.log(LogLevel.ERROR, errMsg);
        			
        		configurationErrorMessages.append("\n");
        		configurationErrorMessages.append(errMsg);

        		erroneousConfiguration = true;
        	}
        		
        	// The specified type must be of allowed types
    		PropertyType typeSpecification = PropertyType.getPropertyType(typeSpec);
        	if(typeSpecification == null) {
        		String errMsg = Messages.getString("PROPERTY_ATTRIBUTE_TYPE_UNKNOWN_TYPE", new Object[] { typeSpec });	//$NON-NLS-1$
        		logger.log(LogLevel.ERROR, errMsg);

        		configurationErrorMessages.append("\n");
        		configurationErrorMessages.append(errMsg);
        			
        		erroneousConfiguration = true;
        	}
        		
        	// The type of the referenced attribute must match the specified type
        	if(attributeIdx != -1 && typeSpecification != null) {
            	Attribute	attribute	= streamSchema.getAttribute(attributeIdx);
            	Type 		attrType	= attribute.getType();

            	if(! PropertyType.doTypeSpecAndAttributeTypeMatch(typeSpecification, attrType)) {
           			String errMsg = Messages.getString("PROPERTY_ATTRIBUTE_TYPE_ATTRIBUTE_TYPE_NONMATCH", new Object[] { typeSpec, attributeName, attrType.getLanguageType() });	//$NON-NLS-1$
           			logger.log(LogLevel.ERROR, errMsg);

           			configurationErrorMessages.append("\n");
           			configurationErrorMessages.append(errMsg);
            			
           			erroneousConfiguration = true;
           		}
       		}
        		
       		// Check if every property name was just used once
       		if(usedPropertyNames.contains(propertyName)) {
       			String errMsg = Messages.getString("PROPERTY_ATTRIBUTE_TYPE_SAME_PROP_NAMES", new Object[] { propertyName });	//$NON-NLS-1$
       			logger.log(LogLevel.ERROR, errMsg);

       			configurationErrorMessages.append("\n");
       			configurationErrorMessages.append(errMsg);
        			
       			erroneousConfiguration = true;
       		}
        		
       		// Check if every attribute name was just used once
       		if(usedAttributeNames.contains(attributeName)) {
       			String errMsg = Messages.getString("PROPERTY_ATTRIBUTE_TYPE_SAME_ATTRIB_NAMES", new Object[] { attributeName });	//$NON-NLS-1$
       			logger.log(LogLevel.ERROR, errMsg);

       			configurationErrorMessages.append("\n");
       			configurationErrorMessages.append(errMsg);
        			
       			erroneousConfiguration = true;
       		}
        		
       		// Do not use current configuration, if it is erroneous
       		if(erroneousConfiguration) {
       			throwJmsHeaderPropertiesConfigurationException = true;
       			continue;
       		}
        		
       		// Store used property & attribute names
       		usedPropertyNames.add(propertyName);
       		usedAttributeNames.add(attributeName);
        		
       		PropertyAttributeType pat = new PropertyAttributeType(propertyName, attributeName, attributeIdx, typeSpecification);
       		patTriplets.add(pat);
   		}

    	if(throwJmsHeaderPropertiesConfigurationException) {
    		throw new JmsHeaderPropertiesConfigurationException(configurationErrorMessages.toString());
    	}
	}
	
	
}
