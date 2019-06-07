package azurerm

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/hashicorp/terraform/helper/acctest"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
)

func TestAccAzureRMStreamAnalyticsOutputDatalake_avro(t *testing.T) {
	resourceName := "azurerm_stream_analytics_output_datalake.test"
	ri := tf.AccRandTimeInt()
	rs := acctest.RandString(5)
	location := testLocation()
	azureConfig := testGetAzureConfig(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAzureRMStreamAnalyticsOutputDatalakeDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAzureRMStreamAnalyticsOutputDatalake_avro(ri, rs, location, azureConfig.TenantID),
				Check: resource.ComposeTestCheckFunc(
					testCheckAzureRMStreamAnalyticsOutputDatalakeExists(resourceName),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
				ImportStateVerifyIgnore: []string{
					// not returned from the API
					"refresh_token",
				},
			},
		},
	})
}

func TestAccAzureRMStreamAnalyticsOutputDatalake_csv(t *testing.T) {
	resourceName := "azurerm_stream_analytics_output_datalake.test"
	ri := tf.AccRandTimeInt()
	rs := acctest.RandString(5)
	location := testLocation()
	azureConfig := testGetAzureConfig(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAzureRMStreamAnalyticsOutputDatalakeDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAzureRMStreamAnalyticsOutputDatalake_csv(ri, rs, location, azureConfig.TenantID),
				Check: resource.ComposeTestCheckFunc(
					testCheckAzureRMStreamAnalyticsOutputDatalakeExists(resourceName),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
				ImportStateVerifyIgnore: []string{
					// not returned from the API
					"refresh_token",
				},
			},
		},
	})
}

func TestAccAzureRMStreamAnalyticsOutputDatalake_json(t *testing.T) {
	resourceName := "azurerm_stream_analytics_output_datalake.test"
	ri := tf.AccRandTimeInt()
	rs := acctest.RandString(5)
	location := testLocation()
	azureConfig := testGetAzureConfig(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAzureRMStreamAnalyticsOutputDatalakeDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAzureRMStreamAnalyticsOutputDatalake_json(ri, rs, location, azureConfig.TenantID),
				Check: resource.ComposeTestCheckFunc(
					testCheckAzureRMStreamAnalyticsOutputDatalakeExists(resourceName),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
				ImportStateVerifyIgnore: []string{
					// not returned from the API
					"refresh_token",
				},
			},
		},
	})
}

func TestAccAzureRMStreamAnalyticsOutputDatalake_update(t *testing.T) {
	resourceName := "azurerm_stream_analytics_output_datalake.test"
	ri := tf.AccRandTimeInt()
	rs := acctest.RandString(5)
	location := testLocation()
	azureConfig := testGetAzureConfig(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAzureRMStreamAnalyticsOutputDatalakeDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAzureRMStreamAnalyticsOutputDatalake_json(ri, rs, location, azureConfig.TenantID),
				Check: resource.ComposeTestCheckFunc(
					testCheckAzureRMStreamAnalyticsOutputDatalakeExists(resourceName),
				),
			},
			{
				Config: testAccAzureRMStreamAnalyticsOutputDatalake_updated(ri, rs, location, azureConfig.TenantID),
				Check: resource.ComposeTestCheckFunc(
					testCheckAzureRMStreamAnalyticsOutputDatalakeExists(resourceName),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
				ImportStateVerifyIgnore: []string{
					// not returned from the API
					"refresh_token",
				},
			},
		},
	})
}

func TestAccAzureRMStreamAnalyticsOutputDatalake_requiresImport(t *testing.T) {
	if !requireResourcesToBeImported {
		t.Skip("Skipping since resources aren't required to be imported")
		return
	}

	resourceName := "azurerm_stream_analytics_output_datalake.test"
	ri := tf.AccRandTimeInt()
	rs := acctest.RandString(5)
	location := testLocation()
	azureConfig := testGetAzureConfig(t)

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAzureRMStreamAnalyticsOutputDatalakeDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccAzureRMStreamAnalyticsOutputDatalake_json(ri, rs, location, azureConfig.TenantID),
				Check: resource.ComposeTestCheckFunc(
					testCheckAzureRMStreamAnalyticsOutputDatalakeExists(resourceName),
				),
			},
			{
				Config:      testAccAzureRMStreamAnalyticsOutputDatalake_requiresImport(ri, rs, location, azureConfig.TenantID),
				ExpectError: testRequiresImportError("azurerm_stream_analytics_output_datalake"),
			},
		},
	})
}

func testCheckAzureRMStreamAnalyticsOutputDatalakeExists(resourceName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		// Ensure we have enough information in state to look up in API
		rs, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return fmt.Errorf("Not found: %s", resourceName)
		}

		name := rs.Primary.Attributes["name"]
		jobName := rs.Primary.Attributes["stream_analytics_job_name"]
		resourceGroup := rs.Primary.Attributes["resource_group_name"]

		conn := testAccProvider.Meta().(*ArmClient).streamAnalyticsOutputsClient
		ctx := testAccProvider.Meta().(*ArmClient).StopContext
		resp, err := conn.Get(ctx, resourceGroup, jobName, name)
		if err != nil {
			return fmt.Errorf("Bad: Get on streamAnalyticsOutputsClient: %+v", err)
		}

		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("Bad: Stream Output %q (Stream Analytics Job %q / Resource Group %q) does not exist", name, jobName, resourceGroup)
		}

		return nil
	}
}

func testCheckAzureRMStreamAnalyticsOutputDatalakeDestroy(s *terraform.State) error {
	conn := testAccProvider.Meta().(*ArmClient).streamAnalyticsOutputsClient

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "azurerm_stream_analytics_output_datalake" {
			continue
		}

		name := rs.Primary.Attributes["name"]
		jobName := rs.Primary.Attributes["stream_analytics_job_name"]
		resourceGroup := rs.Primary.Attributes["resource_group_name"]
		ctx := testAccProvider.Meta().(*ArmClient).StopContext
		resp, err := conn.Get(ctx, resourceGroup, jobName, name)
		if err != nil {
			return nil
		}

		if resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("Stream Analytics Output Datalake still exists:\n%#v", resp.OutputProperties)
		}
	}

	return nil
}

func testAccAzureRMStreamAnalyticsOutputDatalake_avro(rInt int, rString string, location string, tenantId string) string {
	template := testAccAzureRMStreamAnalyticsOutputDatalake_template(rInt, rString, location)
	return fmt.Sprintf(`
%s

resource "azurerm_stream_analytics_output_datalake" "test" {
  name                      = "acctestoutput-%d"
  stream_analytics_job_name = "${azurerm_stream_analytics_job.test.name}"
  resource_group_name       = "${azurerm_stream_analytics_job.test.resource_group_name}"
  account_name 				= "${azurerm_data_lake_store.test.name}"
  tenant_id					= "%s"
  token_user_principal_name = "Principal.Name"
  token_user_display_name 	= "Principal Display Name"
  refresh_token 			= "dummy"
  file_path_prefix 			= "landing/{date}/{time}"
  date_format               = "yyyy-MM-dd"
  time_format               = "HH"

  serialization {
    type = "Avro"
  }
}
`, template, rInt, tenantId)
}

func testAccAzureRMStreamAnalyticsOutputDatalake_csv(rInt int, rString string, location string, tenantId string) string {
	template := testAccAzureRMStreamAnalyticsOutputDatalake_template(rInt, rString, location)
	return fmt.Sprintf(`
%s

resource "azurerm_stream_analytics_output_datalake" "test" {
  name                      = "acctestoutput-%d"
  stream_analytics_job_name = "${azurerm_stream_analytics_job.test.name}"
  resource_group_name       = "${azurerm_stream_analytics_job.test.resource_group_name}"
  account_name 				= "${azurerm_data_lake_store.test.name}"
  tenant_id					= "%s"
  token_user_principal_name = "Principal.Name"
  token_user_display_name 	= "Principal Display Name"
  refresh_token 			= "dummy"
  file_path_prefix 			= "landing/{date}/{time}"
  date_format               = "yyyy-MM-dd"
  time_format               = "HH"

  serialization {
    type            = "Csv"
    encoding        = "UTF8"
    field_delimiter = ","
  }
}
`, template, rInt, tenantId)
}

func testAccAzureRMStreamAnalyticsOutputDatalake_json(rInt int, rString string, location string, tenantId string) string {
	template := testAccAzureRMStreamAnalyticsOutputDatalake_template(rInt, rString, location)
	return fmt.Sprintf(`
%s

resource "azurerm_stream_analytics_output_datalake" "test" {
  name                      = "acctestoutput-%d"
  stream_analytics_job_name = "${azurerm_stream_analytics_job.test.name}"
  resource_group_name       = "${azurerm_stream_analytics_job.test.resource_group_name}"
  account_name 				= "${azurerm_data_lake_store.test.name}"
  tenant_id					= "%s"
  token_user_principal_name = "Principal.Name"
  token_user_display_name 	= "Principal Display Name"
  refresh_token 			= "dummy"
  file_path_prefix 			= "landing/{date}/{time}"
  date_format               = "yyyy-MM-dd"
  time_format               = "HH"

  serialization {
    type     = "Json"
    encoding = "UTF8"
    format   = "LineSeparated"
  }
}
`, template, rInt, tenantId)
}

func testAccAzureRMStreamAnalyticsOutputDatalake_updated(rInt int, rString string, location string, tenantId string) string {
	template := testAccAzureRMStreamAnalyticsOutputDatalake_template(rInt, rString, location)
	return fmt.Sprintf(`
%s

resource "azurerm_data_lake_store" "test" {
	name 				= "accteststore%s"
	resource_group_name = "${azurerm_resource_group.test.name}"
	location 			= "${azurerm_resource_group.test.location}"
	encryption_state 	= "Enabled"
	encryption_type 	= "ServiceManaged"
  }

resource "azurerm_stream_analytics_output_datalake" "test" {
  name                      = "acctestoutput-%d"
  stream_analytics_job_name = "${azurerm_stream_analytics_job.test.name}"
  resource_group_name       = "${azurerm_stream_analytics_job.test.resource_group_name}"
  account_name 				= "${azurerm_data_lake_store.test.name}"
  tenant_id					= "%s"
  token_user_principal_name = "Principal.Name"
  token_user_display_name 	= "Principal Display Name"
  refresh_token 			= "dummy"
  file_path_prefix 			= "landing/{date}/{time}"
  date_format               = "yyyy-MM-dd"
  time_format               = "HH"

  serialization {
    type = "Avro"
  }
}
`, template, rString, rInt, tenantId)
}

func testAccAzureRMStreamAnalyticsOutputDatalake_requiresImport(rInt int, rString string, location string, tenantId string) string {
	template := testAccAzureRMStreamAnalyticsOutputDatalake_json(rInt, rString, location, tenantId)
	return fmt.Sprintf(`
%s

resource "azurerm_stream_analytics_output_datalake" "import" {
  name                      = "${azurerm_stream_analytics_output_datalake.test.name}"
  stream_analytics_job_name = "${azurerm_stream_analytics_output_datalake.test.stream_analytics_job_name}"
  resource_group_name       = "${azurerm_stream_analytics_output_datalake.test.resource_group_name}"
  account_name 				= "${azurerm_stream_analytics_output_datalake.test.account_name}"
  tenant_id					= "${azurerm_stream_analytics_output_datalake.test.tenant_id}"
  token_user_principal_name = "${azurerm_stream_analytics_output_datalake.test.token_user_principal_name}"
  token_user_display_name 	= "${azurerm_stream_analytics_output_datalake.test.token_user_display_name}"
  refresh_token 			= "${azurerm_stream_analytics_output_datalake.test.refresh_token}"
  file_path_prefix 			= "${azurerm_stream_analytics_output_datalake.test.file_path_prefix}"
  date_format               = "${azurerm_stream_analytics_output_datalake.test.date_format}"
  time_format               = "${azurerm_stream_analytics_output_datalake.test.time_format}"
  serialization             = "${azurerm_stream_analytics_output_datalake.test.serialization}"
}
`, template)
}

func testAccAzureRMStreamAnalyticsOutputDatalake_template(rInt int, rString string, location string) string {
	return fmt.Sprintf(`
resource "azurerm_resource_group" "test" {
  name     = "acctestRG-%d"
  location = "%s"
}

resource "azurerm_data_lake_store" "test" {
  name 					= "accteststore%s"
  resource_group_name 	= "${azurerm_resource_group.test.name}"
  location 				= "${azurerm_resource_group.test.location}"
  encryption_state 		= "Enabled"
  encryption_type 		= "ServiceManaged"
}

resource "azurerm_stream_analytics_job" "test" {
  name                                     = "acctestjob-%d"
  resource_group_name                      = "${azurerm_resource_group.test.name}"
  location                                 = "${azurerm_resource_group.test.location}"
  compatibility_level                      = "1.0"
  data_locale                              = "en-GB"
  events_late_arrival_max_delay_in_seconds = 60
  events_out_of_order_max_delay_in_seconds = 50
  events_out_of_order_policy               = "Adjust"
  output_error_policy                      = "Drop"
  streaming_units                          = 3

  transformation_query = <<QUERY
    SELECT *
    INTO [YourOutputAlias]
    FROM [YourInputAlias]
QUERY
}
`, rInt, location, rString, rInt)
}
