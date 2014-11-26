package ch.unibas.cs.dbis;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import oracle.jdbc.xa.OracleXid;
import oracle.jdbc.xa.client.OracleXADataSource;

public class XATest {

	private static final String ORACLE_CONNECTION_STRING2 = "jdbc:oracle:thin:@p7.cs.unibas.ch:1521:orclp7";
	private static final String ORACLE_CONNECTION_STRING = "jdbc:oracle:thin:@p6.cs.unibas.ch:1521:orclp6";
	private static final String ORACLE_USERNAME = "CS341_3";	
	private static final String ORACLE_PASSWORD = "F3NNcYjK";


	// The Oracle XA data source objects
	private static OracleXADataSource oxa1 = null;
	private static OracleXADataSource oxa2 = null;

	// The XA Ressource objects
	private static XAResource xares1 = null;
	private static XAResource xares2 = null;

	// The XA connections
	private static XAConnection xacon1 = null;
	private static XAConnection xacon2 = null;

	// The transaction identifiers
	private static Xid xid1 = null;
	private static Xid xid2 = null;

	// The JDBC connections
	private static Connection connection1 = null;
	private static Connection connection2 = null;

	// Some flags to indicate if the execution of the transaction branch was
	// successful.
	private static boolean branch1_OK = true;
	private static boolean branch2_OK = true;

	// Variables for storing the result of prepare to commit
	private static int prepareResultBranch1 = -1;
	private static int prepareResultBranch2 = -1;
	private static int result1 = 0;
	private static int result2 = 0;
	
	private static double amount = 100.50;
	private static String IBAN1 = "CH5367A1";
	private static String IBAN2 = "CH5367A2";

	/**
	 * Creates a unique Transaction Id for each branch. Transaction Id will be
	 * identical for all the branches under the same distributed transaction.
	 * Branch Id is unique for each branch under the same distributed
	 * transaction.
	 **/
	static Xid createXid(int transId, int branchId) throws XAException {

		// The identifiers are 64 bytes each
		byte[] globalTransId = new byte[64];
		byte[] branchQualifier = new byte[64];

		// Here we use only 1 byte for the exercise
		globalTransId[0] = (byte) transId;
		branchQualifier[0] = (byte) branchId;

		// Create the Oracle Transaction Id
		// Transaction Id has 3 components
		Xid xid = new OracleXid(	0x1234, 		// Format identifier
				globalTransId, 	// Global transaction identifier
				branchQualifier); // Branch qualifier

		return xid;
	}

	/**
	 * The main method
	 */
	public static void main(String[] args) {		
		
		transaction();
		
	}

	static void transaction() {
		System.out.println();
		System.out.println("================Start transaction====================");
		System.out.println();
		if(!initializeFirstTransactionBranch()){
			System.out.println();
			System.out.println("=================End transaction===================");
			System.out.println();
			return;
		}
		if(!startFirstTransactionBranch()){
			System.out.println();
			System.out.println("=================End transaction===================");
			System.out.println();
			return;
		}
		executeQueryForFirstBranch();
		endOfFirstTransactionBranch();

		
		if(!initializeSecondTransactionBranch()){
			System.out.println();
			System.out.println("=================End transaction===================");
			System.out.println();
			return;
		}
		if(!startSecondTransactionBranch()){
			System.out.println();
			System.out.println("=================End transaction===================");
			System.out.println();
			return;
		}
		executeQueryForSecondBranch();
		endOfSecondTransactionBranch();

		/*
		 * Transfer of coordination: The coordinator writes the begin log here and sends the coordinator role to the agents.
		 * Presumed abort: The coordinator writes the begin log and send the actor the prepare-to-commit.
		 */
		prepareToCommitFirstBranch();
		prepareToCommitSecondBranch();

		/*
		 * Transfer of coordination: The agent checks if he can commit, writes the log and give the coordinator role back.
		 * Presumed abort: The agent commits locally and sends success to the coordinator.
		 */
		commitOrRollback();
		/*
		 * Transfer of coordination: The coordinator checks if all agent can commit. Writes the logs and sends an ack to every agent that he may commit now.
		 * Presumed abort: A timeout occurred at the agent. Agent wants to know if he may really commit, the coordinator sends an abort, because it assumes something went wrong.
		 */

		closeFirstConnection();
		closeSecondConnection();
		
		System.out.println();
		System.out.println("=================End transaction===================");
		System.out.println();
	}

	private static void closeSecondConnection() {
		// Finally we close the connections
		try {
			connection2.close();
			xacon2.close();
		} catch (SQLException e) {
			System.err
			.println("Error on closing connections " + e.getMessage());
			e.printStackTrace();
		}
	}

	private static void closeFirstConnection() {
		// Finally we close the connections
		try {
			connection1.close();
			xacon1.close();
		} catch (SQLException e) {
			System.err
			.println("Error on closing connections " + e.getMessage());
			e.printStackTrace();
		}
	}

	private static void commitOrRollback() {
		// Now the final decision: if all branches are successful go for commit on
		// all branches.
		// Attention here only one branch is evaluated!
		try {
			System.out.println("Result of prepare to commit of first branch: "
					+ prepareResultBranch1);
			System.out.println("Result of prepare to commit of second branch: "
					+ prepareResultBranch2);
			// Prepare OK => Commit
			if (prepareResultBranch1 == XAResource.XA_OK && result1 > 0 && prepareResultBranch2 == XAResource.XA_OK && result2 > 0){
				System.out.println("Commit of all branches!");
				xares1.commit(xid1, false);
				xares2.commit(xid2, false);
				// Read only transactions do not need commit or rollback
			} else if (prepareResultBranch1 == XAResource.XA_RDONLY && result1 > 0 && prepareResultBranch2 == XAResource.XA_RDONLY && result2 > 0) {
				System.out.println("Only read operations are executed. No Need for rollback.");
//				xares1.rollback(xid1);
//				xares2.rollback(xid2);
			}else{
				System.out.println("Rollback of all branches due to failures!");
				xares1.rollback(xid1);
				xares2.rollback(xid2);
			}
		} catch (XAException e) {
			System.out.println("XA Commit/Rollback not possible! "
					+ e.errorCode);
		}
	}

	private static void prepareToCommitSecondBranch() {
		// Other branches Prepare to commit here.
		try {
			if (branch2_OK) {
				System.out
				.println("Executing prepare to commit of second branch");
				prepareResultBranch2 = xares2.prepare(xid2);
			} else {
				System.out
				.println("No prepare to commit of second branch because branch was not successful");
			}
		} catch (XAException e) {
			System.out.println("Fehler in prepare to commit! " + e.errorCode);
		}
	}

	private static void prepareToCommitFirstBranch() {
		// Finally we come the core of 2PhaseCommit protocol.
		// First the prepare to commit of first branch (only if successful).
		try {
			if (branch1_OK) {
				System.out
				.println("Executing prepare to commit of first branch");
				prepareResultBranch1 = xares1.prepare(xid1);
			} else {
				System.out
				.println("No prepare to commit of first branch because branch was not successful");
			}
		} catch (XAException e) {
			System.out.println("Fehler in prepare to commit! " + e.errorCode);
		}
	}

	private static void endOfSecondTransactionBranch() {
		// End of the second transaction branch
		try {
			System.out
			.println("The second XA branch has finished execution with result: "
					+ branch2_OK + " and changed "+ result1 + " rows.");
			if (branch2_OK)
				xares2.end(xid2, XAResource.TMSUCCESS);
			else
				xares2.end(xid2, XAResource.TMFAIL);
		} catch (XAException e) {
			System.out.println("Exception on ending second XA branch! "
					+ e.errorCode);
		}
	}

	private static void executeQueryForSecondBranch() {
		// Here the actual work of the transaction branch is performed.
		// Attention: you have to remember if SQL statements in a branch were
		// successful or not.
		try {
			Statement stmt2 = connection2.createStatement();
			String sql2 = "UPDATE account SET balance = balance - "+amount+" WHERE IBAN='"+IBAN2+"'";
			result2 = stmt2.executeUpdate(sql2);
		} catch (SQLException e) {
			// Error in SQL statement
			branch2_OK = false;
			System.out.println("Error in SQL statement of second branch! "
					+ e.getMessage());
		}
	}

	private static boolean startSecondTransactionBranch() {
		// Start of the second XA Branch
		try {
			xares2.start(xid2, XAResource.TMNOFLAGS);
			return true;
		} catch (Exception e) {
			System.out.println("Error during start of the second branch! "
					+ e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	private static boolean initializeSecondTransactionBranch() {
		// Other XA branches may be inserted here
		// or if you like you can also implement a multithreaded version where
		// branches are executed in parallel.
		// Initialisation of the second transaction branch
		try {
			// Connect to the database
			oxa2 = new OracleXADataSource();
			oxa2.setURL(ORACLE_CONNECTION_STRING2);
			oxa2.setUser(ORACLE_USERNAME);
			oxa2.setPassword(ORACLE_PASSWORD);

			// get a XA connection
			xacon2 = oxa2.getXAConnection();

			// get a normal JDBC connection
			connection2 = xacon2.getConnection();
			// Do not use autocommit for SQL operations
			connection2.setAutoCommit(false);

			// Create a XAResource object for the given XA connection
			xares2 = xacon2.getXAResource();

			// Look for pending transaction branches
			Xid xids[] = xares2.recover(XAResource.TMSTARTRSCAN);
			System.out.println("Found " + xids.length
					+ " pending transaction branches!");

			// Perform a Rollback of all pending transaction branches
			for (int i = 0; i < xids.length; i++) {
				System.out.println("Rollback of transaction branch XID: "
						+ xids[i].getGlobalTransactionId()[0] + ":"
						+ xids[i].getBranchQualifier()[0]);
				xares2.rollback(xids[i]);
				i++;
			}

			// Create a new transaction ID for this branch. Transaction: 19
			// branch: 2
			xid2 = createXid(19, 2);
			return true;

		} catch (Exception e) {
			System.err
			.println("Exception during initialisation of second branch! "
					+ e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	private static void endOfFirstTransactionBranch() {
		// End of the first transaction branch
		try {
			System.out
			.println("The first XA branch has finished execution with result: "
					+ branch1_OK + " and changed "+ result1 + " rows.");
			if (branch1_OK)
				xares1.end(xid1, XAResource.TMSUCCESS);
			else
				xares1.end(xid1, XAResource.TMFAIL);
		} catch (XAException e) {
			System.out.println("Exception on ending first XA branch! "
					+ e.errorCode);
		}
	}

	private static void executeQueryForFirstBranch() {
		// Here the actual work of the transaction branch is performed.
		// Attention: you have to remember if SQL statements in a branch were
		// successful or not.
		try {
			Statement stmt1 = connection1.createStatement();
			String sql1 = "UPDATE account SET balance = balance +"+ amount+" WHERE IBAN='"+IBAN1+"'";
			result1 = stmt1.executeUpdate(sql1);
		} catch (SQLException e) {
			// Error in SQL statement
			branch1_OK = false;
			System.out.println("Error in SQL statement of first branch! "
					+ e.getMessage());
		}
	}

	private static boolean startFirstTransactionBranch() {
		// Start of the first XA Branch
		try {
			xares1.start(xid1, XAResource.TMNOFLAGS);
			return true;
		} catch (Exception e) {
			System.out.println("Error during start of the first branch! "
					+ e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	private static boolean initializeFirstTransactionBranch() {
		// Initialisation of the first transaction branch
		try {
			// Connect to the database
			oxa1 = new OracleXADataSource();
			oxa1.setURL(ORACLE_CONNECTION_STRING);
			oxa1.setUser(ORACLE_USERNAME);
			oxa1.setPassword(ORACLE_PASSWORD);

			// get a XA connection
			xacon1 = oxa1.getXAConnection();

			// get a normal JDBC connection
			connection1 = xacon1.getConnection();
			// Do not use autocommit for SQL operations
			connection1.setAutoCommit(false);

			// Create a XAResource object for the given XA connection
			xares1 = xacon1.getXAResource();

			// Look for pending transaction branches
			Xid xids[] = xares1.recover(XAResource.TMSTARTRSCAN);
			System.out.println("Found " + xids.length
					+ " pending transaction branches!");

			// Perform a Rollback of all pending transaction branches
			for (int i = 0; i < xids.length; i++) {
				System.out.println("Rollback of transaction branch XID: "
						+ xids[i].getGlobalTransactionId()[0] + ":"
						+ xids[i].getBranchQualifier()[0]);
				xares1.rollback(xids[i]);
				i++;
			}

			// Create a new transaction ID for this branch. Transaction: 19
			// branch: 1
			xid1 = createXid(19, 1);
			return true;
		} catch (Exception e) {
			System.err
			.println("Exception during initialisation of first branch! "
					+ e.getMessage());
			e.printStackTrace();
			return false;
		}
	}

	public static String getIBAN1() {
		return IBAN1;
	}

	public static void setIBAN1(String iBAN1) {
		IBAN1 = iBAN1;
	}
	
	public static String getIBAN2() {
		return IBAN2;
	}

	public static void setIBAN2(String iBAN2) {
		IBAN2 = iBAN2;
	}
	
	public static double getAmount() {
		return amount;
	}

	public static void setAmount(double amount_) {
		amount = amount_;
	}

}
