package vault

import (
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/hashicorp/terraform-provider-vault/internal/provider"
)

const replicationPath = "/sys/replication/"

func replicationPrimaryConfigResource() *schema.Resource {

	return &schema.Resource{
		Create: replicationPrimaryCreate,
		Read:   ReadWrapper(replicationPrimaryRead),
		Delete: replicationPrimaryDelete,
		Exists: replicationPrimaryExists,
		// Importer: &schema.ResourceImporter{
		// 	State: schema.ImportStatePassthrough,
		// },

		Schema: map[string]*schema.Schema{
			"type": {
				Type:        schema.TypeString,
				ForceNew:    true,
				Required:    true,
				Description: "Type of replication to configure.",
			},
			"primary_cluster_addr": {
				Type:        schema.TypeString,
				ForceNew:    true,
				Optional:    true,
				Description: "Primary cluster address.",
			},
			"cluster_id": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Cluster ID.",
			},
			"mode": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Replication mode (primary or secondary).",
			},
			"state": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Replication state.",
			},
			"known_secondaries": {
				Type:        schema.TypeSet,
				Computed:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "Discovered secondary cluster nodes.",
			},
			"secondaries": {
				Type:        schema.TypeSet,
				Computed:    true,
				Description: "Configured secondaries.",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"api_address": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"cluster_address": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"connection_status": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"last_heartbeat": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"node_id": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
			},
		},
	}
}

func replicationPrimaryCreatePath(typeValue string) string {
	return replicationPath + typeValue + "/primary/enable"
}

func replicationPrimaryReadPath(typeValue string) string {
	return replicationPath + typeValue + "/status"
}

func replicationPrimaryDeletePath(typeValue string) string {
	return replicationPath + typeValue + "/primary/disable"
}

func waitForReplication(state string, path string, d *schema.ResourceData, meta interface{}) error {
	log.Printf("[DEBUG] Waiting for replication state to be %s", state)
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return e
	}

	for {
		resp, _ := client.Logical().Read(path)
		if val, ok := resp.Data["state"]; ok {
			log.Printf("[DEBUG] Replication state: %s", val)
			if resp.Data["state"] == state {
				break
			} else {
				log.Printf("[DEBUG] Replication state: %s", val)
				time.Sleep(1 * time.Second)
			}
		}
	}

	return nil
}

func replicationPrimaryCreate(d *schema.ResourceData, meta interface{}) error {
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return e
	}

	typeValue := d.Get("type").(string)
	path := replicationPrimaryCreatePath(typeValue)
	primaryClusterAddr := d.Get("primary_cluster_addr").(string)

	var data map[string]interface{}
	if primaryClusterAddr != "" {
		data = map[string]interface{}{
			"primary_cluster_addr": primaryClusterAddr,
		}
	}

	resp, err := client.Logical().Write(path, data)
	if err != nil {
		return fmt.Errorf("error enabling %s replication: %s", typeValue, err)
	}
	if resp == nil {
		log.Printf("[DEBUG] Response from client was nil")
	} else {
		if error, ok := resp.Data["Errors"]; ok {
			return fmt.Errorf("error enabling %s replication: %s", typeValue, error)
		}
	}

	log.Printf("[DEBUG] Replication (%s) enabled", typeValue)
	d.SetId(path)
	path = replicationPrimaryReadPath(typeValue)
	waitForReplication("running", path, d, meta)

	return replicationPrimaryRead(d, meta)
}

func replicationPrimaryRead(d *schema.ResourceData, meta interface{}) error {
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return e
	}
	typeValue := d.Get("type").(string)
	path := replicationPrimaryReadPath(typeValue)

	resp, err := client.Logical().Read(path)

	if err != nil {
		log.Printf("[DEBUG] error reading: %v", resp.Data)
		return err
	} else {
		log.Printf("[DEBUG] Read %s: %v", path, resp.Data)
	}

	if resp.Data["mode"] == "disabled" {
		log.Printf("[DEBUG] Replication disabled, removing from state")
		d.SetId("")
	}

	d.Set("primary_cluster_addr", resp.Data["primary_cluster_addr"].(string))
	d.Set("cluster_id", resp.Data["cluster_id"].(string))
	d.Set("mode", resp.Data["mode"].(string))
	d.Set("state", resp.Data["state"].(string))
	d.Set("known_secondaries", resp.Data["known_secondaries"].([]interface{}))
	d.Set("secondaries", resp.Data["secondaries"].([]interface{}))

	return nil
}

func replicationPrimaryDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[DEBUG] Deleteing replication configuration")
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return e
	}

	typeValue := d.Get("type").(string)
	path := replicationPrimaryDeletePath(typeValue)

	resp, err := client.Logical().Write(path, nil)
	if err != nil {
		return fmt.Errorf("error disabling %s replication: %s", typeValue, err)
	}

	if resp.Data["Errors"] != nil {
		return fmt.Errorf("error disabling %s replication: %s", typeValue, resp.Data["Errors"])
	}

	return nil
}

func replicationPrimaryExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	log.Printf("[DEBUG] Checking if replication configuration exists")
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return false, e
	}

	typeValue := d.Get("type").(string)
	path := replicationPrimaryReadPath(typeValue)

	resp, err := client.Logical().Read(path)
	if err != nil {
		return true, fmt.Errorf("error checking %s replication: %s", typeValue, err)
	}

	if resp.Data["Errors"] != nil {
		return true, fmt.Errorf("error checking %s replication: %s", typeValue, resp.Data["Errors"])
	}

	log.Printf("[DEBUG] Replication (%s) is %s", typeValue, resp.Data["state"])

	if resp.Data["mode"] != "disabled" {
		return true, nil
	} else {
		return false, nil
	}
}
