package azurerm

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/servicebus/mgmt/2017-04-01/servicebus"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/azure"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/features"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func resourceArmServiceBusGeoDRConfiguration() *schema.Resource {
	return &schema.Resource{
		Create: resourceArmServiceBusGeoDRConfigurationCreateUpdate,
		Read:   resourceArmServiceBusGeoDRConfigurationRead,
		Update: resourceArmServiceBusGeoDRConfigurationCreateUpdate,
		Delete: resourceArmServiceBusGeoDRConfigurationDelete,

		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"namespace_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: azure.ValidateServiceBusNamespaceName(),
			},

			"resource_group_name": azure.SchemaResourceGroupName(),

			"partner_namespace_id": {
				Type:     schema.TypeString,
				Required: true,
			},
		},
	}
}

func resourceArmServiceBusGeoDRConfigurationCreateUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*ArmClient).ServiceBus.DisasterRecoveryConfigsClient
	ctx := meta.(*ArmClient).StopContext

	name := d.Get("name").(string)
	namespaceName := d.Get("namespace_name").(string)
	resourceGroupName := d.Get("resource_group_name").(string)
	partnerNamespaceId := d.Get("partner_namespace_id").(string)

	if features.ShouldResourcesBeImported() && d.IsNewResource() {
		existing, err := client.Get(ctx, resourceGroupName, namespaceName, name)
		if err != nil {
			if !utils.ResponseWasNotFound(existing.Response) {
				return fmt.Errorf("Error checking for presence of existing ServiceBus Geo DR Configuration %q (Resource Group %q): %+v", name, resourceGroupName, err)
			}
		}

		if existing.ID != nil && *existing.ID != "" {
			return tf.ImportAsExistsError("azurerm_servicebus_geo_dr_configuration", *existing.ID)
		}
	}

	namespacesClient := meta.(*ArmClient).ServiceBus.NamespacesClient
	namespace, err := namespacesClient.Get(ctx, resourceGroupName, namespaceName)
	if err != nil {
		return fmt.Errorf("Error retrieving ServiceBus Namespace %q (Resource Group %q): %+v", namespaceName, resourceGroupName, err)
	}

	if namespace.Sku.Name == servicebus.Premium {
		return fmt.Errorf("Geo DR Configuration is only supported for ServiceBus Premium SKU")
	}

	parameters := servicebus.ArmDisasterRecovery{
		ArmDisasterRecoveryProperties: &servicebus.ArmDisasterRecoveryProperties{
			PartnerNamespace: &partnerNamespaceId,
		},
	}

	if _, err = client.CreateOrUpdate(ctx, resourceGroupName, namespaceName, name, parameters); err != nil {
		return fmt.Errorf("Error creating/updating ServiceBus Geo DR Configuration %q (Resource Group %q): %+v", name, resourceGroupName, err)
	}

	stateConf := &resource.StateChangeConf{
		Pending:                   []string{"Accepted"},
		Target:                    []string{"Succeeded"},
		Refresh:                   serviceBusGeoDRConfigurationRefreshFunc(ctx, client, resourceGroupName, namespaceName, name),
		Timeout:                   30 * time.Minute,
		MinTimeout:                1 * time.Minute,
		ContinuousTargetOccurence: 5,
	}

	if _, err = stateConf.WaitForState(); err != nil {
		return fmt.Errorf("Error waiting for ServiceBus Geo DR Configuration %q (Resource Group %q) to be created or updated: %+v", name, resourceGroupName, err)
	}

	resp, err := client.Get(ctx, resourceGroupName, namespaceName, name)
	if err != nil {
		return fmt.Errorf("Error retrieving ServiceBus Geo DR Configuration %q (Resource Group %q): %+v", name, resourceGroupName, err)
	}

	d.SetId(*resp.ID)

	return resourceArmServiceBusGeoDRConfigurationRead(d, meta)
}

func serviceBusGeoDRConfigurationRefreshFunc(ctx context.Context, client *servicebus.DisasterRecoveryConfigsClient, resourceGroupName string, namespaceName string, name string) resource.StateRefreshFunc {
	return func() (interface{}, string, error) {
		resp, err := client.Get(ctx, resourceGroupName, namespaceName, name)

		if err != nil {
			if utils.ResponseWasNotFound(resp.Response) {
				return nil, "ResponseNotFound", nil
			}

			return nil, "", fmt.Errorf("Error polling for the state of the ServiceBus Geo DR Configuration %q (Resource Group %q): %+v", name, resourceGroupName, err)
		}

		return resp, string(resp.ProvisioningState), nil
	}
}
