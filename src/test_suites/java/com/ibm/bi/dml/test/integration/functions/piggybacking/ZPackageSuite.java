package com.ibm.bi.dml.test.integration.functions.piggybacking;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/** Group together the tests in this package into a single suite so that the Maven build
 *  won't run two of them at once. */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	PiggybackingTest1.class,
	PiggybackingTest2.class
})


/** This class is just a holder for the above JUnit annotations. */
public class ZPackageSuite {

}