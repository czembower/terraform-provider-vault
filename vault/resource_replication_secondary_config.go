package vault

import (
	"fmt"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/hashicorp/terraform-provider-vault/internal/provider"
)

func replicationSecondaryConfigResource() *schema.Resource {

	return &schema.Resource{
		Create: replicationSecondaryCreate,
		Read:   ReadWrapper(replicationSecondaryRead),
		Delete: replicationSecondaryDelete,
		Exists: replicationSecondaryExists,
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
			"token": {
				Type:        schema.TypeString,
				ForceNew:    true,
				Required:    true,
				Description: "Secondary replication token.",
			},
			"primary_api_addr": {
				Type:        schema.TypeString,
				ForceNew:    true,
				Optional:    true,
				Description: "Primary API address.",
			},
			"ca_file": {
				Type:        schema.TypeString,
				ForceNew:    true,
				Optional:    true,
				Description: "Path to local CA file for validating the primary cluster.",
			},
			"ca_path": {
				Type:        schema.TypeString,
				ForceNew:    true,
				Optional:    true,
				Description: "Path to local CA directory for validating the primary cluster.",
			},
			"primary_cluster_addr": {
				Type:        schema.TypeString,
				Computed:    true,
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
			"known_primary_cluster_addrs": {
				Type:        schema.TypeSet,
				Computed:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "Discovered primary cluster nodes.",
			},
			"primaries": {
				Type:        schema.TypeSet,
				Computed:    true,
				Description: "Configured primaries.",
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
					},
				},
			},
		},
	}
}

func replicationSecondaryCreatePath(typeValue string) string {
	return replicationPath + typeValue + "/secondary/enable"
}

func replicationSecondaryReadPath(typeValue string) string {
	return replicationPath + typeValue + "/status"
}

func replicationSecondaryDeletePath(typeValue string) string {
	return replicationPath + typeValue + "/secondary/disable"
}

func replicationSecondaryCreate(d *schema.ResourceData, meta interface{}) error {
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return e
	}

	typeValue := d.Get("type").(string)
	path := replicationSecondaryCreatePath(typeValue)
	replicationToken := d.Get("token").(string)
	primaryApiAddr := d.Get("primary_api_addr").(string)
	caFile := d.Get("ca_file").(string)
	caPath := d.Get("ca_path").(string)

	data := map[string]interface{}{
		"token":            replicationToken,
		"primary_api_addr": primaryApiAddr,
		"ca_file":          caFile,
		"ca_path":          caPath,
	}

	resp, err := client.Logical().Write(path, data)
	if err != nil {
		return fmt.Errorf("error enabling %s replication: %s", typeValue, err)
	}
	if resp == nil {
		log.Printf("[DEBUG] Response from client was nil")
	} else {
		if err, ok := resp.Data["Errors"]; ok {
			return fmt.Errorf("error returned from %s primary: %s", typeValue, err)
		}
	}

	log.Printf("[DEBUG] Replication (%s) enabled", typeValue)
	d.SetId(path)
	path = replicationSecondaryReadPath(typeValue)
	// waitForReplication("stream-wals", path, d, meta)

	return replicationSecondaryRead(d, meta)
}

func replicationSecondaryRead(d *schema.ResourceData, meta interface{}) error {
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return e
	}
	typeValue := d.Get("type").(string)
	path := replicationSecondaryReadPath(typeValue)

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
	d.Set("known_primary_cluster_addrs", resp.Data["known_primary_cluster_addrs"].([]interface{}))
	d.Set("primaries", resp.Data["primaries"].([]interface{}))

	return nil
}

func replicationSecondaryDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[DEBUG] Deleteing replication configuration")
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return e
	}

	typeValue := d.Get("type").(string)
	path := replicationSecondaryDeletePath(typeValue)

	resp, err := client.Logical().Write(path, nil)
	if err != nil {
		return fmt.Errorf("error disabling %s replication: %s", typeValue, err)
	}

	if resp.Data["Errors"] != nil {
		return fmt.Errorf("error disabling %s replication: %s", typeValue, resp.Data["Errors"])
	}

	return nil
}

func replicationSecondaryExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	log.Printf("[DEBUG] Checking if replication configuration exists")
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return false, e
	}

	typeValue := d.Get("type").(string)
	path := replicationSecondaryReadPath(typeValue)

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
