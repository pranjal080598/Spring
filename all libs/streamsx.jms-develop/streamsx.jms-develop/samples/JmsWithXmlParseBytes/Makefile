# Copyright (C)2014, International Business Machines Corporation and  
# others. All Rights Reserved.                        
.PHONY: all clean

STREAMS_JMS_TOOLKIT ?= ../../com.ibm.streamsx.jms:${STREAMS_INSTALL}/toolkits/com.ibm.streamsx.jms

SPLC_FLAGS ?= -a -t ${STREAMS_JMS_TOOLKIT} --data-directory=data

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE = application::JmsWithXmlParseBytes
OUTPUT_DIR = output

all:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(OUTPUT_DIR) $(SPL_CMD_ARGS)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)


