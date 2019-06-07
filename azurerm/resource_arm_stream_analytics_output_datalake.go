package azurerm

import (
	"fmt"
	"log"

	"github.com/Azure/azure-sdk-for-go/services/streamanalytics/mgmt/2016-03-01/streamanalytics"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/azure"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/response"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/validate"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func resourceArmStreamAnalyticsOutputDataLake() *schema.Resource {
	return &schema.Resource{
		Create: resourceArmStreamAnalyticsOutputDataLakeCreateUpdate,
		Read:   resourceArmStreamAnalyticsOutputDataLakeRead,
		Update: resourceArmStreamAnalyticsOutputDataLakeCreateUpdate,
		Delete: resourceArmStreamAnalyticsOutputDataLakeDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.NoEmptyStrings,
			},

			"stream_analytics_job_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.NoEmptyStrings,
			},

			"resource_group_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.NoEmptyStrings,
			},

			"account_name": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validate.NoEmptyStrings,
			},

			"tenant_id": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validate.NoEmptyStrings,
			},

			"token_user_principal_name": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validate.NoEmptyStrings,
			},

			"token_user_display_name": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validate.NoEmptyStrings,
			},

			"refresh_token": {
				Type:      schema.TypeString,
				Required:  true,
				Sensitive: true,
				// This needs to have a dummy value on creation, and then Renewed on Azure Portal.
				ValidateFunc: validate.NoEmptyStrings,
			},

			"file_path_prefix": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validate.NoEmptyStrings,
			},

			"date_format": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validate.NoEmptyStrings,
			},

			"time_format": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validate.NoEmptyStrings,
			},

			"serialization": azure.SchemaStreamAnalyticsOutputSerialization(),
		},
	}
}

func resourceArmStreamAnalyticsOutputDataLakeCreateUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).streamAnalyticsOutputsClient
	ctx := meta.(*ArmClient).StopContext

	log.Printf("[INFO] Preparing arguments for Azure Stream Analytics Data Lake Output creation.")
	name := d.Get("name").(string)
	jobName := d.Get("stream_analytics_job_name").(string)
	resourceGroup := d.Get("resource_group_name").(string)

	if requireResourcesToBeImported && d.IsNewResource() {
		existing, err := client.Get(ctx, resourceGroup, jobName, name)
		if err != nil && !utils.ResponseWasNotFound(existing.Response) {
			return fmt.Errorf("Error checking for existing Azure Stream Analytics Data Lake Output %q (Job %q / Resource Group %q): %s", name, jobName, resourceGroup, err)
		}

		if existing.ID != nil && *existing.ID != "" {
			return tf.ImportAsExistsError("azurerm_stream_analytics_output_data_lake", *existing.ID)
		}
	}

	accountName := d.Get("account_name").(string)
	tenantId := d.Get("tenant_id").(string)
	tokenUserPrincipalName := d.Get("token_user_principal_name").(string)
	tokenUserDisplayName := d.Get("token_user_display_name").(string)
	refreshToken := d.Get("refresh_token").(string)
	filePathPrefix := d.Get("file_path_prefix").(string)
	dateFormat := d.Get("date_format").(string)
	timeFormat := "HH"

	serializationRaw := d.Get("serialization").([]interface{})
	serialization, err := azure.ExpandStreamAnalyticsOutputSerialization(serializationRaw)
	if err != nil {
		return fmt.Errorf("Error expanding `serialization`: %+v", err)
	}

	props := streamanalytics.Output{
		Name: utils.String(name),
		OutputProperties: &streamanalytics.OutputProperties{
			Datasource: &streamanalytics.AzureDataLakeStoreOutputDataSource{
				Type: streamanalytics.TypeMicrosoftDataLakeAccounts,
				AzureDataLakeStoreOutputDataSourceProperties: &streamanalytics.AzureDataLakeStoreOutputDataSourceProperties{
					AccountName:            utils.String(accountName),
					TenantID:               utils.String(tenantId),
					TokenUserPrincipalName: utils.String(tokenUserPrincipalName),
					TokenUserDisplayName:   utils.String(tokenUserDisplayName),
					RefreshToken:           utils.String(refreshToken),
					FilePathPrefix:         utils.String(filePathPrefix),
					DateFormat:             utils.String(dateFormat),
					TimeFormat:             utils.String(timeFormat),
				},
			},
			Serialization: serialization,
		},
	}

	if d.IsNewResource() {
		if _, err := client.CreateOrReplace(ctx, props, resourceGroup, jobName, name, "", ""); err != nil {
			return fmt.Errorf("Error Creating Stream Analytics Output Data Lake %q (Job %q / Resource Group %q): %+v", name, jobName, resourceGroup, err)
		}

		read, err := client.Get(ctx, resourceGroup, jobName, name)
		if err != nil {
			return fmt.Errorf("Error retrieving Stream Analytics Output Data Lake %q (Job %q / Resource Group %q): %+v", name, jobName, resourceGroup, err)
		}
		if read.ID == nil {
			return fmt.Errorf("Cannot read ID of Stream Analytics Output Data Lake %q (Job %q / Resource Group %q)", name, jobName, resourceGroup)
		}

		d.SetId(*read.ID)
	} else {
		if _, err := client.Update(ctx, props, resourceGroup, jobName, name, ""); err != nil {
			return fmt.Errorf("Error Updating Stream Analytics Output Data Lake %q (Job %q / Resource Group %q): %+v", name, jobName, resourceGroup, err)
		}
	}

	return resourceArmStreamAnalyticsOutputDataLakeRead(d, meta)
}

func resourceArmStreamAnalyticsOutputDataLakeRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).streamAnalyticsOutputsClient
	ctx := meta.(*ArmClient).StopContext

	id, err := parseAzureResourceID(d.Id())
	if err != nil {
		return err
	}
	resourceGroup := id.ResourceGroup
	jobName := id.Path["streamingjobs"]
	name := id.Path["outputs"]

	resp, err := client.Get(ctx, resourceGroup, jobName, name)
	if err != nil {
		if utils.ResponseWasNotFound(resp.Response) {
			log.Printf("[DEBUG] Output Data Lake %q was not found in Stream Analytics Job %q / Resource Group %q - removing from state!", name, jobName, resourceGroup)
			d.SetId("")
			return nil
		}

		return fmt.Errorf("Error retrieving Stream Output Data Lake %q (Stream Analytics Job %q / Resource Group %q): %+v", name, jobName, resourceGroup, err)
	}

	d.Set("name", name)
	d.Set("resource_group_name", resourceGroup)
	d.Set("stream_analytics_job_name", jobName)

	if props := resp.OutputProperties; props != nil {
		v, ok := props.Datasource.AsAzureDataLakeStoreOutputDataSource()
		if !ok {
			return fmt.Errorf("Error converting Output Data Source to Data Lake Output: %+v", err)
		}

		d.Set("account_name", v.AccountName)
		d.Set("tenant_id", v.TenantID)
		d.Set("token_user_principal_name", v.TokenUserPrincipalName)
		d.Set("token_user_display_name", v.TokenUserDisplayName)
		d.Set("refresh_token", v.RefreshToken)
		d.Set("file_path_prefix", v.FilePathPrefix)
		d.Set("date_format", v.DateFormat)
		d.Set("time_format", v.TimeFormat)

		if err := d.Set("serialization", azure.FlattenStreamAnalyticsOutputSerialization(props.Serialization)); err != nil {
			return fmt.Errorf("Error setting `serialization`: %+v", err)
		}
	}

	return nil
}

func resourceArmStreamAnalyticsOutputDataLakeDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).streamAnalyticsOutputsClient
	ctx := meta.(*ArmClient).StopContext

	id, err := parseAzureResourceID(d.Id())
	if err != nil {
		return err
	}
	resourceGroup := id.ResourceGroup
	jobName := id.Path["streamingjobs"]
	name := id.Path["outputs"]

	if resp, err := client.Delete(ctx, resourceGroup, jobName, name); err != nil {
		if !response.WasNotFound(resp.Response) {
			return fmt.Errorf("Error deleting Output Data Lake %q (Stream Analytics Job %q / Resource Group %q) %+v", name, jobName, resourceGroup, err)
		}
	}

	return nil
}
