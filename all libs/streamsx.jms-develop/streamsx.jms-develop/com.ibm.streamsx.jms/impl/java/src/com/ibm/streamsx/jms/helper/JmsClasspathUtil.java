/*******************************************************************************
 * Copyright (C) 2014, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.jms.helper;

import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streamsx.jms.JMSOpConstants;

public class JmsClasspathUtil {

	private static final Logger tracer = Logger.getLogger(JmsClasspathUtil.class.getName());


	public static void setupClassPaths(OperatorContext context) {
		
		tracer.log(TraceLevel.TRACE, "Setting up classpath"); //$NON-NLS-1$
		
		boolean classpathSet = false;

		
		// Dump the provided environment
		Map<String,String>	sysEnvMap	= System.getenv();
		Set<String>			sysEnvKeys	= sysEnvMap.keySet();
		tracer.log(TraceLevel.TRACE, "------------------------------------------------------------------------------------"); //$NON-NLS-1$
		tracer.log(TraceLevel.TRACE, "--- System Environment used during initialization"); //$NON-NLS-1$
		for( String key : sysEnvKeys) {
			tracer.log(TraceLevel.TRACE, key + " = " + System.getenv(key)); //$NON-NLS-1$
		}
		tracer.log(TraceLevel.TRACE, "------------------------------------------------------------------------------------"); //$NON-NLS-1$


		if(context.getParameterNames().contains(JMSOpConstants.PARAM_CLASS_LIBS))
		{
			List<String> classLibPaths = context.getParameterValues(JMSOpConstants.PARAM_CLASS_LIBS);
			
			for(String path : classLibPaths) {
				
				tracer.log(TraceLevel.TRACE, "Handling " + JMSOpConstants.PARAM_CLASS_LIBS + " value: " + path); //$NON-NLS-1$ //$NON-NLS-2$
				
				String pathToAdd;
				if(path.startsWith("/")) {
					pathToAdd = path;
				}
				else {
					pathToAdd = context.getPE().getApplicationDirectory() + "/" + path; //$NON-NLS-1$
				}
				
				tracer.log(TraceLevel.TRACE, "Trying to load: " + JMSOpConstants.PARAM_CLASS_LIBS + " value: " + pathToAdd); //$NON-NLS-1$
				
				Path pathObj = Paths.get(pathToAdd);
				if(Files.exists(pathObj)) {
					if(Files.isDirectory(pathObj)) {
						pathToAdd += "/*";
					}
					try {
						tracer.log(TraceLevel.TRACE, "Adding passed class lib to context: " + pathToAdd); //$NON-NLS-1$
						context.addClassLibraries(new String[] {pathToAdd});
						classpathSet = true;
					}
					catch (MalformedURLException e) {
						tracer.log(TraceLevel.ERROR, "Failed to add class lib to context: " + e.getMessage()); //$NON-NLS-1$
					}
				}
				else {
					tracer.log(TraceLevel.ERROR, "Does not exist: " + pathToAdd); //$NON-NLS-1$
				}
			}
		}
		else
		{
			String AMQ_HOME = System.getenv("STREAMS_MESSAGING_AMQ_HOME"); //$NON-NLS-1$
			if (AMQ_HOME != null) {
				
				tracer.log(TraceLevel.TRACE, "Apache Active MQ classpath!"); //$NON-NLS-1$

				String lib = AMQ_HOME + "/lib/*"; //$NON-NLS-1$
				String libOptional = AMQ_HOME + "/lib/optional/*"; //$NON-NLS-1$
		
				try {
					tracer.log(TraceLevel.TRACE, "Adding ActiveMQ class libs to context"); //$NON-NLS-1$
					context.addClassLibraries(new String[] { lib, libOptional });
					classpathSet = true;
				}
				catch (MalformedURLException e) {
					tracer.log(TraceLevel.ERROR, "Failed to add class libs to context: " + e.getMessage()); //$NON-NLS-1$
				}
			}
		
			String WMQ_HOME = System.getenv("STREAMS_MESSAGING_WMQ_HOME"); //$NON-NLS-1$
			if (WMQ_HOME != null) {
				
				tracer.log(TraceLevel.TRACE, "IBM IBM MQ classpath!"); //$NON-NLS-1$

				String javaLib = WMQ_HOME + "/java/lib/*"; //$NON-NLS-1$
				try {
					tracer.log(TraceLevel.TRACE, "Adding IBM MQ class libs to context"); //$NON-NLS-1$
					context.addClassLibraries(new String[] { javaLib });
					classpathSet = true;
				}
				catch (MalformedURLException e) {
					tracer.log(TraceLevel.ERROR, "Failed to add class libs to context: " + e.getMessage()); //$NON-NLS-1$
				}
			}
		}
		
		
		if(	classpathSet == false ) {
			tracer.log(TraceLevel.ERROR, "No classpath has been set!"); //$NON-NLS-1$
		}
		
		tracer.log(TraceLevel.TRACE, "Finished setting up classpath!"); //$NON-NLS-1$

	}

}