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

	private static final String ORACLE_CONNECTION_STRING = "jdbc:oracle:thin:@p6.cs.unibas.ch:1521:orclp6";
	private static final String ORACLE_USERNAME = "CS341_3";	
	private static final String ORACLE_PASSWORD = "F3NNcYjK";

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

		// The Oracle XA data source objects
		OracleXADataSource oxa1 = null;
		OracleXADataSource oxa2 = null;

		// The XA Ressource objects
		XAResource xares1 = null;
		XAResource xares2 = null;

		// The XA connections
		XAConnection xacon1 = null;
		XAConnection xacon2 = null;

		// The transaction identifiers
		Xid xid1 = null;
		Xid xid2 = null;

		// The JDBC connections
		Connection connection1 = null;
		Connection connection2 = null;

		// Some flags to indicate if the execution of the transaction branch was
		// successful.
		boolean branch1_OK = true;
		boolean branch2_OK = true;

		// Variables for storing the result of prepare to commit
		int prepareResultBranch1 = -1;
		int prepareResultBranch2 = -1;

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

		} catch (Exception e) {
			System.err
			.println("Exception during initialisation of first branch! "
					+ e.getMessage());
			e.printStackTrace();
			return;
		}

		// Start of the first XA Branch
		try {
			xares1.start(xid1, XAResource.TMNOFLAGS);
		} catch (Exception e) {
			System.out.println("Error during start of the first branch! "
					+ e.getMessage());
			e.printStackTrace();
			return;
		}

		// Here the actual work of the transaction branch is performed.
		// Attention: you have to remember if SQL statements in a branch were
		// successful or not.
		try {
			Statement stmt1 = connection1.createStatement();
			String sql1 = "UPDATE account SET balance = balance + 100.50 WHERE IBAN='CH5367A1'";
			stmt1.executeUpdate(sql1);
		} catch (SQLException e) {
			// Error in SQL statement
			branch1_OK = false;
			System.out.println("Error in SQL statement of first branch! "
					+ e.getMessage());
		}

		// End of the first transaction branch
		try {
			System.out
			.println("The first XA branch has finished execution with result: "
					+ branch1_OK);
			if (branch1_OK)
				xares1.end(xid1, XAResource.TMSUCCESS);
			else
				xares1.end(xid1, XAResource.TMFAIL);
		} catch (XAException e) {
			System.out.println("Exception on ending first XA branch! "
					+ e.errorCode);
		}

		// Other XA branches may be inserted here
		// or if you like you can also implement a multithreaded version where
		// branches are executed in parallel.
		// Initialisation of the second transaction branch
		try {
			// Connect to the database
			oxa2 = new OracleXADataSource();
			oxa2.setURL(ORACLE_CONNECTION_STRING);
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

		} catch (Exception e) {
			System.err
			.println("Exception during initialisation of second branch! "
					+ e.getMessage());
			e.printStackTrace();
			return;
		}

		// Start of the second XA Branch
		try {
			xares2.start(xid2, XAResource.TMNOFLAGS);
		} catch (Exception e) {
			System.out.println("Error during start of the second branch! "
					+ e.getMessage());
			e.printStackTrace();
			return;
		}


		// Here the actual work of the transaction branch is performed.
		// Attention: you have to remember if SQL statements in a branch were
		// successful or not.
		try {
			Statement stmt2 = connection2.createStatement();
			String sql2 = "UPDATE account SET balance = balance + 100.50 WHERE IBAN='CH5367A1'";
			stmt2.executeUpdate(sql2);
		} catch (SQLException e) {
			// Error in SQL statement
			branch1_OK = false;
			System.out.println("Error in SQL statement of second branch! "
					+ e.getMessage());
		}




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

		// Other branches Prepare to commit here.

		// Now the final decision: if all branches are successful go for commit on
		// all branches.
		// Attention here only one branch is evaluated!
		try {
			System.out.println("Result of prepare to commit of first branch: "
					+ prepareResultBranch1);
			// Prepare OK => Commit
			if (prepareResultBranch1 == XAResource.XA_OK) {
				System.out.println("Commit of all branches!");
				xares1.commit(xid1, false);
				// Read only transactions do not need commit or rollback
			} else if (prepareResultBranch1 != XAResource.XA_RDONLY) {
				System.out.println("Rollback of all branches due to failures!");
				xares1.rollback(xid1);
			}
		} catch (XAException e) {
			System.out.println("XA Commit/Rollback not possible! "
					+ e.errorCode);
		}

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

}
