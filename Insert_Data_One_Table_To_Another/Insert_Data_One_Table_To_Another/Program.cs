using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;

namespace Insert_Data_One_Table_To_Another
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Connection strings for source and target databases
            string sourceConnString = "Server=database-prod.geotabreports.com;Database=websitedb;User ID=ssingh;Password=shDB1337#!gt;TrustServerCertificate=True;Connection Timeout=18000";
            string targetConnString = "Server=database-dev.geotabreports.com;Database=websitedb;User ID=ssingh;Password=gp$=db=1337;TrustServerCertificate=True;Connection Timeout=18000";

            // Batch size for inserting records
            int batchSize = 1000;

            try
            {
                // Connect to the target database to retrieve existing records
                HashSet<string> existingRecords = new HashSet<string>();
                using (SqlConnection targetConn = new SqlConnection(targetConnString))
                {
                    targetConn.Open();
                    string selectExistingQuery = "SELECT REGISTRY_ID FROM fuel_supplier_data";
                    using (SqlCommand selectExistingCmd = new SqlCommand(selectExistingQuery, targetConn))
                    using (SqlDataReader existingReader = selectExistingCmd.ExecuteReader())
                    {
                        while (existingReader.Read())
                        {
                            existingRecords.Add(existingReader.GetString(0)); // Assuming REGISTRY_ID is of type string
                        }
                    }
                }

                // Connect to the source database
                using (SqlConnection sourceConn = new SqlConnection(sourceConnString))
                {
                    sourceConn.Open();
                    string selectQuery = "SELECT * FROM fuel_supplier_data";
                    using (SqlCommand selectCmd = new SqlCommand(selectQuery, sourceConn))
                    using (SqlDataReader reader = selectCmd.ExecuteReader())
                    {
                        // Connect to the target database
                        using (SqlConnection targetConn = new SqlConnection(targetConnString))
                        {
                            targetConn.Open();
                            string insertQuery = "INSERT INTO fuel_supplier_data " +
                                "([FRS_FACILITY_DETAIL_REPORT_URL],[REGISTRY_ID],[PRIMARY_NAME],[LOCATION_ADDRESS],[SUPPLEMENTAL_LOCATION],[CITY_NAME],[COUNTY_NAME],[FIPS_CODE]," +
                                "[STATE_CODE],[STATE_NAME],[COUNTRY_NAME],[POSTAL_CODE],[FEDERAL_FACILITY_CODE],[FEDERAL_AGENCY_NAME],[TRIBAL_LAND_CODE],[TRIBAL_LAND_NAME]," +
                                "[CONGRESSIONAL_DIST_NUM],[CENSUS_BLOCK_CODE],[HUC_CODE],[EPA_REGION_CODE],[SITE_TYPE_NAME],[LOCATION_DESCRIPTION],[CREATE_DATE],[UPDATE_DATE]," +
                                "[US_MEXICO_BORDER_IND],[PGM_SYS_ACRNMS],[LATITUDE83],[LONGITUDE83],[CONVEYOR],[COLLECT_DESC],[ACCURACY_VALUE],[REF_POINT_DESC],[HDATUM_DESC],[SOURCE_DESC]) " +
                                "VALUES (@FRS_FACILITY_DETAIL_REPORT_URL, @REGISTRY_ID, @PRIMARY_NAME, @LOCATION_ADDRESS, @SUPPLEMENTAL_LOCATION, @CITY_NAME, @COUNTY_NAME, @FIPS_CODE, " +
                                "@STATE_CODE, @STATE_NAME, @COUNTRY_NAME, @POSTAL_CODE, @FEDERAL_FACILITY_CODE, @FEDERAL_AGENCY_NAME, @TRIBAL_LAND_CODE, @TRIBAL_LAND_NAME, " +
                                "@CONGRESSIONAL_DIST_NUM, @CENSUS_BLOCK_CODE, @HUC_CODE, @EPA_REGION_CODE, @SITE_TYPE_NAME, @LOCATION_DESCRIPTION, @CREATE_DATE, @UPDATE_DATE, " +
                                "@US_MEXICO_BORDER_IND, @PGM_SYS_ACRNMS, @LATITUDE83, @LONGITUDE83, @CONVEYOR, @COLLECT_DESC, @ACCURACY_VALUE, @REF_POINT_DESC, @HDATUM_DESC, @SOURCE_DESC)";

                            using (SqlCommand insertCmd = new SqlCommand(insertQuery, targetConn))
                            {
                                insertCmd.CommandTimeout = 18000; // Set command timeout

                                int batchCount = 0;

                                while (reader.Read())
                                {
                                    string registryId = reader["REGISTRY_ID"].ToString();
                                    if (!existingRecords.Contains(registryId))
                                    {
                                        insertCmd.Parameters.Clear(); // Clear previous parameters
                                        insertCmd.Parameters.AddWithValue("@FRS_FACILITY_DETAIL_REPORT_URL", reader["FRS_FACILITY_DETAIL_REPORT_URL"]);
                                        insertCmd.Parameters.AddWithValue("@REGISTRY_ID", registryId);
                                        insertCmd.Parameters.AddWithValue("@PRIMARY_NAME", reader["PRIMARY_NAME"]);
                                        insertCmd.Parameters.AddWithValue("@LOCATION_ADDRESS", reader["LOCATION_ADDRESS"]);
                                        insertCmd.Parameters.AddWithValue("@SUPPLEMENTAL_LOCATION", reader["SUPPLEMENTAL_LOCATION"]);
                                        insertCmd.Parameters.AddWithValue("@CITY_NAME", reader["CITY_NAME"]);
                                        insertCmd.Parameters.AddWithValue("@COUNTY_NAME", reader["COUNTY_NAME"]);
                                        insertCmd.Parameters.AddWithValue("@FIPS_CODE", reader["FIPS_CODE"]);
                                        insertCmd.Parameters.AddWithValue("@STATE_CODE", reader["STATE_CODE"]);
                                        insertCmd.Parameters.AddWithValue("@STATE_NAME", reader["STATE_NAME"]);
                                        insertCmd.Parameters.AddWithValue("@COUNTRY_NAME", reader["COUNTRY_NAME"]);
                                        insertCmd.Parameters.AddWithValue("@POSTAL_CODE", reader["POSTAL_CODE"]);
                                        insertCmd.Parameters.AddWithValue("@FEDERAL_FACILITY_CODE", reader["FEDERAL_FACILITY_CODE"]);
                                        insertCmd.Parameters.AddWithValue("@FEDERAL_AGENCY_NAME", reader["FEDERAL_AGENCY_NAME"]);
                                        insertCmd.Parameters.AddWithValue("@TRIBAL_LAND_CODE", reader["TRIBAL_LAND_CODE"]);
                                        insertCmd.Parameters.AddWithValue("@TRIBAL_LAND_NAME", reader["TRIBAL_LAND_NAME"]);
                                        insertCmd.Parameters.AddWithValue("@CONGRESSIONAL_DIST_NUM", reader["CONGRESSIONAL_DIST_NUM"]);
                                        insertCmd.Parameters.AddWithValue("@CENSUS_BLOCK_CODE", reader["CENSUS_BLOCK_CODE"]);
                                        insertCmd.Parameters.AddWithValue("@HUC_CODE", reader["HUC_CODE"]);
                                        insertCmd.Parameters.AddWithValue("@EPA_REGION_CODE", reader["EPA_REGION_CODE"]);
                                        insertCmd.Parameters.AddWithValue("@SITE_TYPE_NAME", reader["SITE_TYPE_NAME"]);
                                        insertCmd.Parameters.AddWithValue("@LOCATION_DESCRIPTION", reader["LOCATION_DESCRIPTION"]);
                                        insertCmd.Parameters.AddWithValue("@CREATE_DATE", reader["CREATE_DATE"]);
                                        insertCmd.Parameters.AddWithValue("@UPDATE_DATE", reader["UPDATE_DATE"]);
                                        insertCmd.Parameters.AddWithValue("@US_MEXICO_BORDER_IND", reader["US_MEXICO_BORDER_IND"]);
                                        insertCmd.Parameters.AddWithValue("@PGM_SYS_ACRNMS", reader["PGM_SYS_ACRNMS"]);
                                        insertCmd.Parameters.AddWithValue("@LATITUDE83", reader["LATITUDE83"]);
                                        insertCmd.Parameters.AddWithValue("@LONGITUDE83", reader["LONGITUDE83"]);
                                        insertCmd.Parameters.AddWithValue("@CONVEYOR", reader["CONVEYOR"]);
                                        insertCmd.Parameters.AddWithValue("@COLLECT_DESC", reader["COLLECT_DESC"]);
                                        insertCmd.Parameters.AddWithValue("@ACCURACY_VALUE", reader["ACCURACY_VALUE"]);
                                        insertCmd.Parameters.AddWithValue("@REF_POINT_DESC", reader["REF_POINT_DESC"]);
                                        insertCmd.Parameters.AddWithValue("@HDATUM_DESC", reader["HDATUM_DESC"]);
                                        insertCmd.Parameters.AddWithValue("@SOURCE_DESC", reader["SOURCE_DESC"]);

                                        insertCmd.ExecuteNonQuery();

                                        batchCount++;
                                        if (batchCount % batchSize == 0)
                                        {
                                            Console.WriteLine($"Inserted {batchCount} records.");
                                        }
                                    }
                                }

                                Console.WriteLine($"Finished inserting {batchCount} records.");
                            }
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                Console.WriteLine("A SQL error occurred: " + ex.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred: " + e.Message);
            }
        }
    }
}
