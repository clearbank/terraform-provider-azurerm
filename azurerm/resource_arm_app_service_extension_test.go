package azurerm

// import (
// 	"testing"

// 	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
// )

// func TestAccAzureRMAppServiceExtension_basic(t *testing.T) {
// 	resourceName := "azurerm_app_service_extension.test"
// 	ri := tf.AccRandTimeInt()
// 	config := testAccAzureRMAppServiceExtension_basic(ri, testLocation())

// 	resource.ParallelTest(t, resource.TestCase{
// 		PreCheck:     func() { testAccPreCheck(t) },
// 		Providers:    testAccProviders,
// 		CheckDestroy: testCheckAzureRMAppServiceExtensionDestroy,
// 		Steps: []resource.TestStep{
// 			{
// 				Config: config,
// 				Check: resource.ComposeTestCheckFunc(
// 					testCheckAzureRMAppServiceExtensionExists(resourceName),
// 					resource.TestCheckResourceAttrSet(resourceName, "name"),
// 					resource.TestCheckResourceAttrSet(resourceName, "app_service_name"),
// 				),
// 			},
// 			{
// 				ResourceName:      resourceName,
// 				ImportState:       true,
// 				ImportStateVerify: true,
// 			},
// 		},
// 	})
// }
