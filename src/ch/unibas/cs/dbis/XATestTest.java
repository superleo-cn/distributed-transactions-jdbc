package ch.unibas.cs.dbis;

import static org.junit.Assert.*;

import org.junit.Test;

public class XATestTest {

	@Test
	public void testTransaction() {
		XATest.transaction();
	}
	
	@Test
	public void testTransactionInvalidAccount(){
		XATest.setIBAN1("CH7777A1");
		XATest.transaction();
	}
	
	@Test
	public void testTransactionInvalidValue(){
		XATest.setIBAN1("CH5367A1");
		XATest.transaction();
	}

}
