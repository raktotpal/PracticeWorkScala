package com.internal.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

/**
 * Insert all dtgroups into GV_AC_ALL_DTGROUPS_ARRAY
 * Split two cases:
 * 		* when dtgroup_list = '*' --> runs for EACH store (called in Select_Donor procedure)
 *   	* when dtgroup_list <>'*' --> runs ONCE (called in load_xcopy procedure)
 *
 * @author RAKTOTPAL
 *
 */
object VLD_XCOPY {
  case class exc_no_data_ostore() extends Exception

  val conf = new SparkConf().setAppName("VLD_XCOPY").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)

  val gv_ac_table_suffix: String = null //IT; vld_te_periodicities.get_tablesuffix(gv_nc_periodset);
  var gv_ac_all_dtgroups_array: Array[String] = null
  val gv_nc_periodid: Double = 0 // 1877.0
  val gv_ac_ostore: String = null // 1600078159
  val gv_ac_dstore: String = null

  def Load_All_DTGroups(p_ac_cstore: String) { // param = 1600078159
    val v_rec_dtgroups_cur: String = null
    val v_nc_dtgroup_pos: Long = 0
    var v_ac_dtgroup: String = null
    var i: Int = 0
    val v_rec_dtgroup: String = null

    // gv_ac_all_dtgroups_array.delete;
    // gv_ac_valid_dtgroups_array.delete;

    i = 1
    if (null == gv_ac_table_suffix) {
      val queryToPickFromRawData = s"""SELECT ac_dtgroup FROM VLDRAWDATA_IT_RAWDATA WHERE nc_periodid=$gv_nc_periodid AND ac_nshopid=$p_ac_cstore GROUP BY ac_dtgroup"""
      val resPickFromRawData = hiveContext.sql(queryToPickFromRawData)
      gv_ac_all_dtgroups_array = new Array[String](resPickFromRawData.count.toInt)
      for (v_rec_dtgroups_cur <- resPickFromRawData) {
        v_ac_dtgroup = v_rec_dtgroups_cur.getString(1)
        // gv_ac_all_dtgroups_array(i).ac_word = v_ac_dtgroup;
        gv_ac_all_dtgroups_array(i) = v_ac_dtgroup;
        i = i + 1
      }
    }

    val ac_dtgroup_query = s"""SELECT ac_dtgroup FROM VLDRAWDATA_IT_RAWDATA  
        						where nc_periodid=$gv_nc_periodid 
        						AND ac_nshopid=$p_ac_cstore GROUP BY ac_dtgroup"""
    val ac_dtgroup_res = hiveContext.sql(ac_dtgroup_query)
    gv_ac_all_dtgroups_array = new Array[String](ac_dtgroup_res.count.toInt)
    for (v_rec_dtgroups_cur <- ac_dtgroup_res) {
      v_ac_dtgroup = v_rec_dtgroups_cur.getString(1)
      gv_ac_all_dtgroups_array(i) = v_ac_dtgroup
      i = i + 1;
    }
  }

  def Insert_ML_Specfc(p_ac_dtgroup: String) { // param: VOLUMETRIC
    val v_ac_procname = "Insert_ML_Specfc"
    var v_infotxt: String = null
    var v_nc_num: Long = 0
    var v_ac_sql: String = null

    if (null == gv_ac_table_suffix) {
      if (p_ac_dtgroup.equals("VOLUMETRIC") || p_ac_dtgroup.equals("VOL_RCC")) {
        val insertSelectQuery = s"""INSERT INTO VLDPROCESS_IT_GT_RAWDATA 
		                      (nc_periodid, ac_nshopid, ac_cref, ac_crefsuffix, ac_dtgroup,
		                       nc_hash_signature, ac_creftype, ac_xcodegr, ac_xcodegrmatch,
		                       f_nan_key, nc_conv, ac_crefstatus, ac_subtag, nc_slot1, nc_slot2,
		                       nc_slot3, nc_slot4, nc_slot5, nc_slot6, nc_slot7, nc_slot8,
		                       nc_slot9, nc_slot10, ac_cslot1, ac_cslot2) 
		             SELECT 
		                    r.nc_periodid, $gv_ac_dstore, r.ac_cref, r.ac_crefsuffix,
		                    r.ac_dtgroup, r.nc_hash_signature, r.ac_creftype, r.ac_xcodegr,
		                    r.ac_xcodegrmatch, r.f_nan_key, r.nc_conv, r.ac_crefstatus,
		                    r.ac_subtag, r.nc_slot1, r.nc_slot2, r.nc_slot3, r.nc_slot4,
		                    r.nc_slot5, r.nc_slot6, r.nc_slot7, r.nc_slot8, r.nc_slot9,
		                    r.nc_slot10, r.ac_cslot1, r.ac_cslot2 
		               FROM VLDRAWDATA_IT_RAWDATA r, VLDIMDB_IT_NANS n, VLDPROCESS_IT_GT_MODULES m 
		              WHERE r.ac_nshopid = $gv_ac_ostore 
		                AND r.nc_periodid = $gv_nc_periodid 
		                AND r.ac_dtgroup = $p_ac_dtgroup 
		                AND r.f_nan_key = n.f_nan_key 
		                AND n.nc_moduleid = m.nc_moduleid"""
        val insertSelectResDF = hiveContext.sql(insertSelectQuery)
        v_nc_num = insertSelectResDF.count
      } else {
        val insertSelectQuery = s"""INSERT INTO VLDPROCESS_IT_GT_RAWDATA 
                      (nc_periodid, ac_nshopid, ac_cref, ac_crefsuffix, ac_dtgroup,
                       nc_hash_signature, ac_creftype, ac_xcodegr, ac_xcodegrmatch,
                       f_nan_key, nc_conv, ac_crefstatus, ac_subtag, nc_slot1, nc_slot2,
                       nc_slot3, nc_slot4, nc_slot5, nc_slot6, nc_slot7, nc_slot8,
                       nc_slot9, nc_slot10, ac_cslot1, ac_cslot2) 
		             SELECT 
		                    r.nc_periodid, $gv_ac_dstore, r.ac_cref, r.ac_crefsuffix,
		                    r.ac_dtgroup, r.nc_hash_signature, r.ac_creftype, r.ac_xcodegr,
		                    r.ac_xcodegrmatch, r.f_nan_key, r.nc_conv, r.ac_crefstatus,
		                    r.ac_subtag, r.nc_slot1, r.nc_slot2, r.nc_slot3, r.nc_slot4,
		                    r.nc_slot5, r.nc_slot6, r.nc_slot7, r.nc_slot8, r.nc_slot9,
		                    r.nc_slot10, r.ac_cslot1, r.ac_cslot2 
		               FROM VLDRAWDATA_IT_RAWDATA r, VLDIMDB_IT_NANS n, VLDPROCESS_IT_GT_MODULES m 
		              WHERE r.ac_nshopid = $gv_ac_ostore 
		                AND r.nc_periodid = $gv_nc_periodid 
		                AND r.ac_dtgroup = $p_ac_dtgroup 
		                AND r.f_nan_key = n.f_nan_key 
		                AND n.nc_moduleid = m.nc_moduleid"""
        val insertSelectResDF = hiveContext.sql(insertSelectQuery)
        v_nc_num = insertSelectResDF.count
      }
    } else {
      v_ac_sql = s"""INSERT INTO VLDPROCESS_IT_GT_RAWDATA 
	                 (nc_periodid, ac_nshopid, ac_cref, ac_crefsuffix, ac_dtgroup,
	                 nc_hash_signature, ac_creftype, ac_xcodegr, ac_xcodegrmatch,
	                 f_nan_key, nc_conv, ac_crefstatus, ac_subtag, nc_slot1, nc_slot2,
	                 nc_slot3, nc_slot4, nc_slot5, nc_slot6, nc_slot7, nc_slot8,
	                 nc_slot9, nc_slot10, ac_cslot1, ac_cslot2) 
                  SELECT 
	                  r.nc_periodid, $gv_ac_dstore , r.ac_cref, r.ac_crefsuffix,
	                  r.ac_dtgroup, r.nc_hash_signature, r.ac_creftype, r.ac_xcodegr,
	            	  r.ac_xcodegrmatch, r.f_nan_key, r.nc_conv, r.ac_crefstatus,
	                  r.ac_subtag, r.nc_slot1, r.nc_slot2, r.nc_slot3, r.nc_slot4,
	                  r.nc_slot5, r.nc_slot6, r.nc_slot7, r.nc_slot8, r.nc_slot9,
	                  r.nc_slot10, r.ac_cslot1, r.ac_cslot2 
                  FROM VLDRAWDATA_IT_RAWDATA r, VLDIMDB_IT_NANS n, VLDPROCESS_IT_GT_MODULES m 
                  WHERE r.ac_nshopid = $gv_ac_ostore 
                  AND r.nc_periodid = $gv_nc_periodid 
                  AND r.ac_dtgroup = $p_ac_dtgroup 
                  AND r.f_nan_key = n.f_nan_key 
                  AND n.nc_moduleid = m.nc_moduleid"""
      val v_ac_sql_resDF = hiveContext.sql(v_ac_sql)
      v_nc_num = v_ac_sql_resDF.count
    }

    v_infotxt = s"""<< $v_ac_procname >> << $p_ac_dtgroup >> $v_nc_num Record(s) Inserted Successfully in GT_Rawdata Table."""

    if (v_nc_num < 1) {
      throw new exc_no_data_ostore
    }

    // vld_process.infomessage (v_infotxt);
  }
}