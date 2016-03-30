package ru.threeg.bank.load;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Logger;

import lv.gcpartners.bank.ClasificData;
import lv.gcpartners.bank.ConnectionFactory;
import lv.gcpartners.bank.Process;

public class load_MZO_v2 extends Process
{
	private DBLoger mDBLoger;
	private int mSessionID;
	private java.sql.Timestamp mStartTime;
	private java.sql.Date mWorkDate;
	private java.sql.Timestamp mSysDate;
	
	private Logger StatisticLog;
	
	private static final String DEF_DATE = "1900-01-01";
	private static final String DEF_TIMESTAMP = "1900-01-01 00:00:00.000000000";
	
	private String mSQLFields_COL_OBJTP = "COLLATOBJECT_CODE, DESCR, BASEVALUATIONTYPE_CODE, TYPEMOVABLEPROPERTY, PR2"
				                       + ", PR3, COLLATAGREEMENTTYPE_CODE, Priority, Revaluation_coeff, Revaluation_dt";
	
	
	
	private String mSQLFields_COL_TP = " COLLATAGREEMENTTYPE_CODE, DESCR, COLLATERAL_CATEGORY, CORE_COLLATERAL, PERIODTYPE_CODE"
									 + " , VALUATIONPERIOD, PARAMETER_LIST, ACCOUNT_ACSQ ";

	
	private String mSQLFields_COLLAT = " CollatAgreementType_Code, CollatAgreement_Number, CollatAgreement_CCY, Factoring_Agr_Number"
									 + ", Branch_ID, Reg_user, Mod_user, Status, CollateralAgreement_ID, Guarantor_ID, CB_Type"
									 + ", Valuation_Amount, Start_Date, End_Date, Reg_date, Mod_date";
	

	private String mSQLFields_AGR_COL = "Mod_User, Reg_user, CollateralDistribution_ID, Collateral_Agr_ID"
			                          + ", Agreement_ID, Part, Part_Calc_Date, Reg_date, Mod_date";
	
	
	private String mSQLFields_COL_OBJ = "CollateralObject_Code, ObjDescr, InsuranceCompany_Code, Insurance_Agr_Number"
		                              + ", Insurance_Beneficiary, IMB_Guarantee_Number, ISIN, BlockedAccount"
		                              + ", Depot_Account, Depot_Agr, Depot_out, DepotAccount_out, Reg_user, Mod_user "
		                              + ", Securiry_Numbers, Cadastral_Num, KLADR_CODE, ship_code, ship_equip_code"
		                              + ", qos_code, RegCountry_code , airplane_code, nk_index, nk_city, nk_Region"
		                              + ", nk_street, class"//27
		                              + ", Collateral_Obj_ID, IsOccupied, CollateralFactor" //3
		                              + ", Readiness, tonnage"//2		                              
		                              + ", Insurance_Period_Since, Insurance_Period_Till, YearOfRelease ,End_Of_Date"//4		                              
		                              + ", Reg_date, Mod_date"; //2		
	
	
	private String mSQLFields_AGR = " Branch_ID, Facility_type, Facility_seq, Facility_CCY, Agreement_Number, Archive_Folder, Narrative,"
								  + " Loan_CCY, Interest_Rate_code, Interest_Rate_Info, Margin_Info, Base_Rate_Code, Loans_maturity,"
								  + " drawdown_account_DEL, drawdown_account_CCY, drawdown_account_ACOD, drawdown_account_ACSQ,"
								  + " drawdown_account_Branch_ID, Prepayment_commission_type, Narrative_2, Prepayment_interest_Type,"
								  + " Narrative_3, Prepayment_account_DEL, Prepayment_account_CCY, Prepayment_account_ACOD, Prepayment_account_ACSQ,"
								  + " Prepayment_account_Branch_ID, Nedooborot_Rate_code, Nedooborot_Rate_Info, Nedooborot_marg_info,"
								  + " Nedooborot_int_period, Nedooborot_repmnt_date, Default_int_code, Default_int_rate_Info, Default_Margin_info,"
								  + " Act_reconciliation_Period, Credit_Product, Profit_Centre, Attention_to_Person, Customer_FAX, Service_Town,"
								  + " Division, Executor, RUR_Account_CCY, RUR_Account_ACOD, RUR_Account_ACSQ, RUR_Account_Branch_ID, RUR_Account_DEL,"
								  + " Last_User,"//49
								  
								  + " Agreement_ID, CNum, Is_Revolving, Is_Multy_CCY, CommonLimit_ID, Is_Facility_Closed, is_Archived,"
							      + " Interest_pmt_day_Number, Is_drawdown_ind, drawdown_account_Customer_ID, Prepayment_account_Customer_ID,"
							      + " Is_BuyCCY_Application, Settlement_period, Commission_ID, is_Manual_FAX, RUR_Account_Customer_ID," //16										      
							
							      + " Facility_Amount, Interest_period, Interest_Rate, Margin, IntRate_Change_Freq, Rpmnt_quantity, MIN_Drawdown,"
							      + " Commission_Percent, RUR_Amount_Percent, Collateral_Percent, Management_Fee, Prepayment_commission_Rate,"
							      + " Prepayment_minimum, Nedooborot_Rate, Default_int_rate,"//15						      
							      
							      + " Document_Receive_Date, Agreement_date, Avail_period_date, Maturity_Date, Facility_Maturity_Date,"
							      + " First_drawdown_Date, First_interest_pmt_date, First_loan_maturity_Date,Input_Work_Date,"
							      + " Change_Work_Date, First_settlement_date," //11
							        
							      + " Input_DateTime, Change_DateTime"; //2
							
	//Ф135 баг #300
	private String mSQLFields_AGR_REG = "Agreement_Number, Reg_user, ReqNum, CCY, Facility_Type, " //5
		                              + " Reg_ID, Customer, BKI, isDelete, " //4
		                              + "Amount, " //1
		                              //0
		                              + "Credit_com_Date, Reg_date";//2
	
	
	private String mSQLFields_COL_LINK = "COLLINKDESCR, REG_USER, MOD_USER, " + //3
			                             "COLLATERAL_LINK_ID, COLLATERAL_OBJ_ID, COLLATERAL_AGR_ID, " + //3
                                         "REG_DATE, MOD_DATE"; //2                  

	
	
	public void RunProcess(java.sql.Date loadDate) throws Exception
    {
        try
        {
        	mDBLoger = new DBLoger();

        	connection = ConnectionFactory.getFactory().getConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(2);
            schemaDWHIN = ConnectionFactory.getFactory().getOption("schema_dwhin");
            schemaDWH = ConnectionFactory.getFactory().getOption("schema_dwh");
            PD = ConnectionFactory.getFactory().getOption("pd");
            PDEXT = ConnectionFactory.getFactory().getOption("pdext");
            BALTUR = ConnectionFactory.getFactory().getOption("baltur");
            clasificator = new ClasificData(connection, schemaDWH, schemaDWHIN, currDay);
            clasificator.loadCurrency();
            currDay = clasificator.getWorkDay();
            
            checkBarsDivDay(currDay);
            connection.commit();
            
            StatisticLog = Logger.getLogger("Statistic");
            
            
            process(loadDate); 
        }
        catch(Exception ex)
        {
            if(connection != null) connection.rollback();
            throw ex;
        }    	
    }
	
	    
    void process(java.sql.Date loadDate) throws Exception
    {
        String sql200_driver = ConnectionFactory.getFactory().getOption("ccd_driver_mzo");
        String sql200_url = ConnectionFactory.getFactory().getOption("ccd_url_mzo");
        String sql200_user = ConnectionFactory.getFactory().getOption("ccd_user_mzo");
        String sql200_pswd = ConnectionFactory.getFactory().getOption("ccd_pswd_mzo");
        
        java.sql.Timestamp fromDate = new java.sql.Timestamp(System.currentTimeMillis());
        //init
        
        int Step = 0;
        
        try
        {
        	
        	StatisticLog.info("MZO_LOAD_STEP: Start");
        	
            String sql;
            PreparedStatement prepare;
            ResultSet rs;
        	
        	//vozmem workDate
            if (loadDate != null) mWorkDate = loadDate;
            else
            {            
		        sql = "Select workday from dwh.workday";
		        prepare = connection.prepareStatement(sql);
		        rs = prepare.executeQuery();            
		        rs.next();
		        
		        mWorkDate = rs.getDate(1);
            }
        	
            this.logger.info("MZO_LOAD_STEP: work date:" + mWorkDate);  
            
            //System.out.println(mWorkDate);
            //mDBLoger.SaveWorkProc("mzo", new java.sql.Timestamp(System.currentTimeMillis()), "T", "test", mWorkDate);
            //--------

            //Proverim status gotovnosti k zagruzke
            Class.forName(sql200_driver);
            Properties prop = new Properties();
            prop.setProperty("user", sql200_user);
            prop.setProperty("password", sql200_pswd);
            
            Connection connectSQL2000 = DriverManager.getConnection(sql200_url, prop);
                  
            //this.logger.info("DB url:" + sql200_url);  
            
            sql = "Select State from va_ORK_LoadingState";
            prepare = connectSQL2000.prepareStatement(sql);
            rs = prepare.executeQuery();            
            rs.next();
            //--   
            
            //esli status ne raven "R" zavrshaem zagruzku i pishem v log
            if (!rs.getString("State").equals("R"))
            {
            	mDBLoger.SaveWorkProc("mzo", new java.sql.Timestamp(System.currentTimeMillis()), "E", "State is not <R> in va_ORK_LoadingState", mWorkDate);
            	this.logger.error("MZO_LOAD_STEP: State is not <R> in va_ORK_LoadingState"); 
            	
            	return;            	
            }            	
            //--          
           
            //vozmem poslednii nomer zagruzki dla zapisi informacii v log            	
        	sql = "Select IFNULL(max(param1) + 1, 1) from dwh.nr_processlog where cbformname = ? and logitemdate = ?";
        	
        	prepare = connection.prepareCall(sql);
			prepare.setString(1,"mzo");
			prepare.setDate(2, new java.sql.Date(System.currentTimeMillis()));
            rs = prepare.executeQuery();                
            rs.next();
        	
            mSessionID = rs.getInt(1);          	
        	//--
            
            CallableStatement callProc;
            //clear stages             
            sql = "{call DWH.PDEL_STAGES}";
    	    callProc = connection.prepareCall(sql);   
    	    callProc.executeQuery();
    	    connection.commit();
    	    
    	    //logger.info("Clear stages complete");  
    	    
            //--
            
            
            //Fill all stage
            mStartTime = new java.sql.Timestamp(System.currentTimeMillis());
        	mDBLoger.SaveWorkProc("mzo", mStartTime, "", "load stages in process", mWorkDate);
        	logger.info("MZO_LOAD_STEP: load stages in process");
        	mDBLoger.DBLog("LOAD STAGES", "mzo", mSessionID, 0, "Load STAGES", "Start load information from RSS");
     	   
            //--        	
        	
        	//Fill stage RSS_STG_COLOBJTP
        	if (!checkOnExecuteWorkDate("COL_OBJTP"))
        	{        	
	        	sql = "Select " + mSQLFields_COL_OBJTP + " from dbo.va_ORK_CollatObjectType";
	            prepare = connectSQL2000.prepareStatement(sql);
	            rs = prepare.executeQuery();            
	            
	            fillStage(rs, "COL_OBJTP", mSQLFields_COL_OBJTP, new int[]{7, 1, 1, 1, 0});
	        	rs.close(); 
        	}        	
        	
        	//logger.info("complete Fill stage RSS_STG_COLOBJTP"); 	
        	//--
        	//Fill stage RSS_STG_COL_TP 
        	if (!checkOnExecuteWorkDate("COL_TP"))
        	{
	        	sql = "Select " + mSQLFields_COL_TP + " from dbo.va_ORK_CollatAgreementType";
	            prepare = connectSQL2000.prepareStatement(sql);
	            rs = prepare.executeQuery();
	
	            fillStage(rs, "COL_TP", mSQLFields_COL_TP, new int[]{5, 2, 1, 0, 0});
	        	rs.close();
        	}
        	//logger.info("complete Fill stage RSS_STG_COL_TP"); 
        	//--
        	//Fill stage RSS_STG_COLLAT 
        	if (!checkOnExecuteWorkDate("COLLAT"))
        	{
	        	sql = "Select " + mSQLFields_COLLAT + " from dbo.va_ORK_CollateralAgreement";
	            prepare = connectSQL2000.prepareStatement(sql);
	            rs = prepare.executeQuery();        	
	        	
	            fillStage(rs, "COLLAT", mSQLFields_COLLAT, new int[]{8, 3, 1, 2, 2});
	        	rs.close();
        	}
        	//logger.info("complete Fill stage COLLAT");
        	//--        	
        	//Fill stage RSS_STG_AGR_COL 
        	if (!checkOnExecuteWorkDate("AGR_COL"))
        	{
	        	sql = "Select " + mSQLFields_AGR_COL + " from dbo.va_ORK_CollateralDistribution";
	            prepare = connectSQL2000.prepareStatement(sql);
	            rs = prepare.executeQuery(); 
	        	
	        	fillStage(rs, "AGR_COL", mSQLFields_AGR_COL, new int[]{2, 3, 1, 1, 2});
	        	rs.close(); 
        	}
        	
        	//logger.info("complete Fill stage AGR_COL");
        	//--
        	//Fill stage RSS_STG_COL_OBJ
        	if (!checkOnExecuteWorkDate("COL_OBJ"))
        	{
	        	sql = "Select " + mSQLFields_COL_OBJ + " from dbo.va_ORK_CollateralObject"; 
	            prepare = connectSQL2000.prepareStatement(sql);
	            rs = prepare.executeQuery();                     
	        	
	            fillStage(rs, "COL_OBJ", mSQLFields_COL_OBJ, new int[]{27, 3, 2, 4, 2});
	        	rs.close();
        	}
        	//logger.info("complete Fill stage COL_OBJ");
        	//--
        	//Fill stage RSS_STG_AGR
        	if (!checkOnExecuteWorkDate("AGR"))
        	{
	        	sql = "Select " + mSQLFields_AGR + " from dbo.va_ORK_Agreement";
	            prepare = connectSQL2000.prepareStatement(sql);
	            
	            rs = prepare.executeQuery();     
	        	 
	            fillStage(rs, "AGR", mSQLFields_AGR, new int[]{49, 16, 15, 11, 2});
	            rs.close();
        	}
        	//logger.info("complete Fill stage AGR");
        	//--
        	//Fill stage RSS_STG_AGR_REG //Ф135 баг #300
        	if (!checkOnExecuteWorkDate("AGR_REG"))
        	{
        		sql = "Select " + mSQLFields_AGR_REG + " from dbo.va_ORK_AgrRegistration";
	            prepare = connectSQL2000.prepareStatement(sql);	            
	            rs = prepare.executeQuery(); 
	        	 
	            fillStage(rs, "AGR_REG", mSQLFields_AGR_REG, new int[]{5, 4, 1, 0, 2});
	            rs.close();
        	}
        	        	
        	if (!checkOnExecuteWorkDate("COL_LINK"))
        	{
        		sql = "Select " + mSQLFields_COL_LINK + " from dbo.vCollateralLink";
	            prepare = connectSQL2000.prepareStatement(sql);	            
	            rs = prepare.executeQuery(); 
	        	 
	            fillStage(rs, "COL_LINK", mSQLFields_COL_LINK, new int[]{3, 3, 0, 0, 2});
	            rs.close();
        	}
        	
        	
        	//*/        	
        	
        	//logger.info("complete Fill stage AGR_REG");
        	//--
        	connectSQL2000.close();  	
        	        	
        	//Zagruzka dannih iz stage v hranilishe
        	
        	Step++;
        	this.logger.info("MZO_LOAD_STEP: fill start");        	
        	mDBLoger.SaveWorkProc("mzo", mStartTime, "", "Zagruzka v STAGE zavershena", mWorkDate);
        	mDBLoger.DBLog("LOAD STAGES", "mzo", mSessionID, 0, "Load STAGES", "zavershena");
      	   
        	
        	
        	sql = "{call DWH.PFILL_AGR_COL}";
    	    callProc = connection.prepareCall(sql);   
    	    callProc.executeQuery();
    	    mDBLoger.SaveWorkProc("mzo", mStartTime, "", "Copyed 1/8 tables", mWorkDate);
    	    mDBLoger.DBLog("DWH.PFILL_AGR_COL", "mzo", mSessionID, 0, "Copyed 1/8 tables", "DWH.PFILL_AGR_COL vipolnena uspeshno");
    	   // logger.info("complete {call DWH.PFILL_AGR_COL");
    	    
    	    Step++;
    	    sql = "{call DWH.PFILL_AGR}";
    	    callProc = connection.prepareCall(sql);   
    	    callProc.executeQuery();
    	    mDBLoger.SaveWorkProc("mzo", mStartTime, "", "Copyed 2/8 tables", mWorkDate);
    	    mDBLoger.DBLog("DWH.PFILL_AGR", "mzo", mSessionID, 0, "Copyed 2/8 tables", "DWH.PFILL_AGR vipolnena uspeshno");
     	   
    	    //logger.info("complete {call DWH.PFILL_AGR");
    	    Step++;
    	    sql = "{call DWH.PFILL_COL_OBJ}";
    	    callProc = connection.prepareCall(sql);   
    	    callProc.executeQuery();
    	    mDBLoger.SaveWorkProc("mzo", mStartTime, "", "Copyed 3/8 tables", mWorkDate);
    	    mDBLoger.DBLog("DWH.PFILL_COL_OBJ", "mzo", mSessionID, 0, "Copyed 3/8 tables", "DWH.PFILL_COL_OBJ vipolnena uspeshno");
      	   
    	   // logger.info("complete {call DWH.PFILL_COL_OBJ");
    	    Step++;
    	    sql = "{call DWH.PFILL_COL_OBJTP}";
    	    callProc = connection.prepareCall(sql);   
    	    callProc.executeQuery();
    	    mDBLoger.SaveWorkProc("mzo", mStartTime, "", "Copyed 4/8 tables", mWorkDate);
    	    mDBLoger.DBLog("DWH.PFILL_COL_OBJTP", "mzo", mSessionID, 0, "Copyed 4/8 tables", "DWH.PFILL_COL_OBJTP vipolnena uspeshno");
       	   
    	   // logger.info("complete {call DWH.PFILL_COL_OBJTP");
    	    Step++;
    	    sql = "{call DWH.PFILL_COL_TP}";
    	    callProc = connection.prepareCall(sql);   
    	    callProc.executeQuery();
    	    mDBLoger.SaveWorkProc("mzo", mStartTime, "", "Copyed 5/8 tables", mWorkDate);
    	    mDBLoger.DBLog("DWH.PFILL_COL_TP", "mzo", mSessionID, 0, "Copyed 5/8 tables", "DWH.PFILL_COL_TP vipolnena uspeshno");
       	   
    	    //logger.info("complete {call DWH.PFILL_COL_TP");
    	    Step++;
    	    sql = "{call DWH.PFILL_COLLAT}";
    	    callProc = connection.prepareCall(sql);   
    	    callProc.executeQuery();
    	    mDBLoger.SaveWorkProc("mzo", mStartTime, "", "Copyed 6/8 tables", mWorkDate);
    	    mDBLoger.DBLog("DWH.PFILL_COLLAT", "mzo", mSessionID, 0, "Copyed 6/8 tables", "DWH.PFILL_COLLAT vipolnena uspeshno");
       	   
    	   // logger.info("complete {call DWH.PFILL_COLLAT");
    	    
    	    //Ф135 баг #300
    	    Step++;
    	    sql = "{call DWH.PFILL_AGR_REG}";
    	    callProc = connection.prepareCall(sql);   
    	    callProc.executeQuery();
    	    mDBLoger.SaveWorkProc("mzo", mStartTime, "", "Copyed 7/8 tables", mWorkDate);
    	    mDBLoger.DBLog("DWH.PFILL_AGR_REG", "mzo", mSessionID, 0, "Copyed 7/8 tables", "DWH.PFILL_AGR_REG vipolnena uspeshno");
       	   
    	    Step++;
    	    sql = "{call DWH.PFILL_COL_LINK}";
    	    callProc = connection.prepareCall(sql);   
    	    callProc.executeQuery();
    	    mDBLoger.SaveWorkProc("mzo", mStartTime, "", "Copyed 8/8 tables", mWorkDate);
    	    mDBLoger.DBLog("DWH.PFILL_COL_LINK", "mzo", mSessionID, 0, "Copyed 8/8 tables", "DWH.PFILL_COL_LINK vipolnena uspeshno");
       	   
    	        	    
    	   // logger.info("complete {call DWH.PFILL_AGR_REG");
    	    //--
    	    //this.logger.info("MZO_LOAD_STEP: fill end");
    	    mDBLoger.SaveWorkProc("mzo", fromDate, "O", "Zagruzka zavershena uspeshno", mWorkDate);
        	logger.info("MZO_LOAD_STEP: Zagruzka zavershena uspeshno");
        	StatisticLog.info("MZO_LOAD_STEP: Zagruzka zavershena uspeshno");
        	//mDBLoger.DBLog("Zagruzka zavershena uspeshno", "mzo", mSessionID, 0, "Copyed 7/7 tables", "DWH.PFILL_AGR_REG vipolnena uspeshno");

    	    //esli zagruzka proshla uspeshno sdelaem commit
    	    connection.commit();
    	    

        }
        catch(Exception ex)
        {
        	connection.rollback();
        	
        	mDBLoger.DBLog("Error on load information", "mzo", mSessionID, 0, "step: " + Step, ex.getMessage());
        	mDBLoger.SaveWorkProc("mzo", fromDate, "E", "Error on load information: " + ex.getMessage(), mWorkDate);
        	logger.error("MZO_LOAD_STEP: Error on load information: " + ex.getMessage());
        	StatisticLog.info("MZO_LOAD_STEP: Error on load information: " + ex.getMessage());
        	throw ex;
        }
    }
    
    /** ZAPOLNENIE STAGE
     * rs - viborka is bazi istochnika
     * stageName - Naznavue hranilisha (Stage)
     * sqlFields - Perechislenie polei s sortirovkoi (string, int, float, date, timestamp)
     * paramsCount - kolichestvo otsortirovannih paramentorv (string, int, float, date, timestamp)
     */    
    private void fillStage(ResultSet rs, String stageName, String sqlFields, int[] paramsCount) throws Exception
    {
    	mSysDate = new java.sql.Timestamp(System.currentTimeMillis());

    	
    	int pc = 0;
    	for (int i = 0; i < paramsCount.length; i++) pc += paramsCount[i];  
    	
    	String sql = "INSERT INTO DWH.RSS_STG_" + stageName + "(" + sqlFields + ", WORKDATE, SYSDATE)" 
        + " VALUES (" + buildParamString(pc + 2) + ")";
    	
		PreparedStatement state = connection.prepareCall(sql);
		int readRecord = 0;
		
		int index = 0;
		
		
		//String _s;
		//int _i;
		//Object obj;
		//_i = null;
		
		try 
		{
			 while(rs.next()) 
			 {
				 				 
				index = 0;
				for (int i = 0; i < paramsCount[0]; i++)
				{
					index++;
					if (rs.getObject(index) == null) state.setString(index, "");
					else state.setString(index, rs.getString(index));					
				}
				
				for (int i = 0; i < paramsCount[1]; i++)
				{
					index++;

					if (rs.getObject(index) == null) state.setInt(index, 0);
					else state.setInt(index, rs.getInt(index));
				}
				
				for (int i = 0; i < paramsCount[2]; i++)
				{
					index++;
					if (rs.getObject(index) == null) state.setFloat(index, 0);
					else state.setFloat(index, rs.getFloat(index));
				}
				
				for (int i = 0; i < paramsCount[3]; i++)
				{
					index++;
					if (rs.getObject(index) == null) state.setDate(index, Date.valueOf(DEF_DATE));
					else state.setDate(index, rs.getDate(index));
				}
				
				for (int i = 0; i < paramsCount[4]; i++)
				{
					index++;
					if (rs.getObject(index) == null) state.setTimestamp(index, Timestamp.valueOf(DEF_TIMESTAMP));
					else state.setTimestamp(index, rs.getTimestamp(index));
				}
				
				index++;
				state.setDate(index, mWorkDate);
				//state.setDate(index, Date.valueOf("2011-01-01"));
				
				index++;
				//state.setTimestamp(index, mSysDate);
				state.setTimestamp(index, mSysDate);
				    
				
			 	state.executeUpdate();
			 	
			 	
			    readRecord++;
				   // System.out.println(stageName + ": OK");	
			}
				 
			connection.commit(); 				    
			logger.info("MZO_LOAD_STEP: Fill stage RSS_STG_" + stageName + " for " + readRecord + " records");
			mDBLoger.DBLog("LOAD STAGES", "mzo", mSessionID, 0, "Load STAGES", "Fill stage RSS_STG_" + stageName + " for " + readRecord + " records");
	     	   
			
			
		}
		catch (Exception e)
		{
			mDBLoger.DBLog("Error on fill stage", "mzo", mSessionID, 0, "RSS_STG_" + stageName, "");
			//System.out.println("Error on fill stage RSS_STG_" + stageName + " field:" + index + "  data:" + rs.getString(index));
			logger.error("MZO_LOAD_STEP: Error on fill stage RSS_STG_" + stageName + " field:" + index);
			throw e;
		}
	}
    
    /** Proverka na sushestvovanie dannih za WorkDate
     * stageName - Naznavue hranilisha (Stage)
     */   
    private boolean checkOnExecuteWorkDate(String stageName) throws Exception
    {
    	String sql = "Select count(*) from DWH.RSS_STG_" + stageName + " where WORKDATE = ?";
    	PreparedStatement prepare = connection.prepareCall(sql);
		prepare.setDate(1, mWorkDate); 
        ResultSet rs = prepare.executeQuery();                
        rs.next();
    	
        if (rs.getInt(1) > 0) return true;
        else return false;    	
    }
    
    
    
	
    public static void main(String str[])
    {
    	System.out.println("Load_MZO_v2 version 1.5");
   	
    	//TimeZone.setDefault(TimeZone.getTimeZone("GMT+3"));
        try
        {       	
        	ConnectionFactory.getFactory().initConnection("connection.properties");
            
        	java.sql.Date date;
        	try 
        	{
        		date = java.sql.Date.valueOf(str[0]);
        	} catch (Exception e)
        	{
        		date = null;
			}
        	
        	
        	load_MZO_v2 load = new load_MZO_v2();    
        	load.RunProcess(date);  	
        	        				       
        }
        catch(Exception ex)
        {        	
        	ex.printStackTrace();
            System.exit(-3);
        }
        
        System.exit(0);
    }
    
    
    private String buildParamString(int count)
    {    	
    	String res = "";
    	
    	if (count < 1) return res;
    	
    	res = "?";    	
    	for (int i = 1; i < count; i++) res += ",?";
    	
    	return res;    	
    }
	
}
