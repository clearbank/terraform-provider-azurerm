package azurerm

import (
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/validate"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func resourceArmAppServiceExtension() *schema.Resource {
	return &schema.Resource{
		Create: resourceArmAppServiceExtensionCreate,
		Read:   resourceArmAppServiceExtensionRead,
		Delete: resourceArmAppServiceExtensionDelete,
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

			"resource_group_name": resourceGroupNameSchema(),

			"app_service_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.NoEmptyStrings,
			},
		},
	}
}

func resourceArmAppServiceExtensionCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).appServicesClient
	ctx := meta.(*ArmClient).StopContext

	name := d.Get("name").(string)
	resourceGroup := d.Get("resource_group_name").(string)
	appServiceName := d.Get("app_service_name").(string)

	azureRMLockByName(appServiceName, appServiceResourceName)
	defer azureRMUnlockByName(appServiceName, appServiceResourceName)

	if requireResourcesToBeImported && d.IsNewResource() {
		existing, err := client.GetSiteExtension(ctx, resourceGroup, appServiceName, name)
		if err != nil {
			if !utils.ResponseWasNotFound(existing.Response) {
				return fmt.Errorf("Error checking for presence of existing Extension %q (App Service %q / Resource Group %q): %+v", name, appServiceName, resourceGroup, err)
			}
		}

		if existing.ID != nil && *existing.ID != "" {
			return tf.ImportAsExistsError("azurerm_app_service_extension", *existing.ID)
		}
	}

	future, err := client.InstallSiteExtension(ctx, resourceGroup, appServiceName, name)
	if err != nil {
		return fmt.Errorf("Error creating Extension %q (App Service %q / Resource Group %q): %+v", name, appServiceName, resourceGroup, err)
	}

	if err = future.WaitForCompletionRef(ctx, client.Client); err != nil {
		return fmt.Errorf("Error waiting for creation of Extension %q (App Service %q / Resource Group %q): %+v", name, appServiceName, resourceGroup, err)
	}

	resp, err := client.GetSiteExtension(ctx, resourceGroup, appServiceName, name)
	if err != nil {
		return fmt.Errorf("Error retrieving Extension %q (App Service %q / Resource Group %q): %+v", name, appServiceName, resourceGroup, err)
	}

	if resp.ID == nil {
		return fmt.Errorf("Cannot read ID for Extension %q (App Service %q / Resource Group %q): %+v", name, appServiceName, resourceGroup, err)
	}
	d.SetId(*resp.ID)
	return resourceArmAppServiceExtensionRead(d, meta)
}

func resourceArmAppServiceExtensionRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).appServicesClient
	ctx := meta.(*ArmClient).StopContext

	id, err := parseAzureResourceID(d.Id())
	if err != nil {
		return fmt.Errorf("Error parsing Azure Resource ID %q: %+v", d.Id(), err)
	}

	name := id.Path["siteextensions"]
	resourceGroup := id.ResourceGroup
	appServiceName := id.Path["sites"]

	resp, err := client.GetSiteExtension(ctx, resourceGroup, appServiceName, name)
	if err != nil {
		if utils.ResponseWasNotFound(resp.Response) {
			log.Printf("[DEBUG] Extension %q (App Service %q / Resource Group %q) was not found - removing from state", name, appServiceName, resourceGroup)
			d.SetId("")
			return nil
		}
		return fmt.Errorf("Error retrieving Extension %q (App Service %q / Resource Group %q): %+v", name, appServiceName, resourceGroup, err)
	}

	d.Set("name", name)
	d.Set("resource_group_name", resourceGroup)
	d.Set("app_service_name", appServiceName)

	return nil
}

func resourceArmAppServiceExtensionDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).appServicesClient
	ctx := meta.(*ArmClient).StopContext

	id, err := parseAzureResourceID(d.Id())
	if err != nil {
		return fmt.Errorf("Error parsing Azure Resource ID %q: %+v", d.Id(), err)
	}

	name := id.Path["siteextensions"]
	resourceGroup := id.ResourceGroup
	appServiceName := id.Path["sites"]

	azureRMLockByName(appServiceName, appServiceResourceName)
	defer azureRMUnlockByName(appServiceName, appServiceResourceName)

	resp, err := client.DeleteSiteExtension(ctx, resourceGroup, appServiceName, name)
	if err != nil {
		if !utils.ResponseWasNotFound(resp) {
			return fmt.Errorf("Error deleting Extension %q (App Service %q / Resource Group %q): %+v", name, appServiceName, resourceGroup, err)
		}
	}

	return nil
}
