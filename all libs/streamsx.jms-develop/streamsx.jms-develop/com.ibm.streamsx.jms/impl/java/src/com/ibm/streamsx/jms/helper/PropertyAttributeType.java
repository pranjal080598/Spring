/*******************************************************************************
 * Copyright (C) 2019, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jms.helper;

import com.ibm.streamsx.jms.types.PropertyType;

/**
 * Helper class to handle data object of a data triplet
 * defining the name of a JMS Header property, the name
 * of the tuple attribute it is mapped to, and the
 * expected/specified type.
 */
public class PropertyAttributeType {

	private String 			propertyName		= null;
	private String 			attributeName		= null;
	private int				attributeIdx		= -1;
	private PropertyType	typeSpecification	= null;
	
	
	/**
	 * Creates a data object of a data triplet defining the
	 * name of a JMS Header property, the name of the attribute
	 * it is mapped to, and the expected/specified type.
	 * 
	 * @param propertyName
	 * @param attributeName
	 * @param attributeIdx
	 * @param typeSpecification
	 */
	public PropertyAttributeType(String propertyName, String attributeName, int attributeIdx, PropertyType typeSpecification) {
		super();
		this.propertyName = propertyName;
		this.attributeName = attributeName;
		this.attributeIdx = attributeIdx;
		this.typeSpecification = typeSpecification;
	}


	public String getPropertyName() {
		return propertyName;
	}


	public String getAttributeName() {
		return attributeName;
	}


	public int getAttributeIdx() {
		return attributeIdx;
	}

	
	public PropertyType getTypeSpecification() {
		return typeSpecification;
	}
	
}
