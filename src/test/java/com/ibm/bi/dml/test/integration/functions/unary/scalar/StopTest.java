/**
 * IBM Confidential
 * OCO Source Materials
 * (C) Copyright IBM Corp. 2010, 2014
 * The source code for this program is not published or otherwise divested of its trade secrets, irrespective of what has been deposited with the U.S. Copyright Office.
 */

package com.ibm.bi.dml.test.integration.functions.unary.scalar;

import org.junit.Test;

import com.ibm.bi.dml.test.integration.AutomatedTestBase;
import com.ibm.bi.dml.test.integration.TestConfiguration;


/**
 * Tests the print function
 */
public class StopTest extends AutomatedTestBase 
{
	@SuppressWarnings("unused")
	private static final String _COPYRIGHT = "Licensed Materials - Property of IBM\n(C) Copyright IBM Corp. 2010, 2014\n" +
                                             "US Government Users Restricted Rights - Use, duplication  disclosure restricted by GSA ADP Schedule Contract with IBM Corp.";
	
	private final static String TEST_DIR = "functions/unary/scalar/";
	private final static String TEST_STOP = "StopTest";
	
	@Override
	public void setUp() {
		baseDirectory = SCRIPT_DIR + "functions/unary/scalar/";

		availableTestConfigurations.put(TEST_STOP, new TestConfiguration(TEST_DIR, TEST_STOP, new String[] {}));
	}

	String errMessage = "M is all 0 matrix.";
	String outMessage = "Sum(M2) = 10.0";
	
	@Test
	public void testStop1_0() {
		runStopTest(1,0);
	}
	
	@Test
	public void testStop1_1() {
		runStopTest(1,1);
	}
	
	public void runStopTest(int test_num, int val) {
		
		TestConfiguration config = availableTestConfigurations.get("StopTest");
		
		String STOP_HOME = SCRIPT_DIR + TEST_DIR;
		fullDMLScriptName = STOP_HOME + TEST_STOP + test_num + ".dml";
		programArgs = new String[]{"-args", Integer.toString(val), errMessage};
		
		loadTestConfiguration(config);
		boolean exceptionExpected = false;
		int expectedNumberOfJobs = 0;
		
		if ( val == 0 )
			setExpectedStdErr(errMessage);
		else 
			setExpectedStdOut(outMessage);
			
		runTest(true, exceptionExpected, null, expectedNumberOfJobs); 
	}
	
}