package vault

import (
	"fmt"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"github.com/hashicorp/terraform-provider-vault/internal/provider"
)

func replicationTokenResource() *schema.Resource {

	return &schema.Resource{
		Create: replicationTokenCreate,
		Read:   ReadWrapper(replicationTokenRead),
		Delete: replicationTokenDelete,
		Exists: replicationTokenExists,
		// Importer: &schema.ResourceImporter{
		// 	State: schema.ImportStatePassthrough,
		// },

		Schema: map[string]*schema.Schema{
			"type": {
				Type:        schema.TypeString,
				ForceNew:    true,
				Required:    true,
				Description: "Type of replication token to create.",
			},
			"token_id": {
				Type:        schema.TypeString,
				ForceNew:    true,
				Required:    true,
				Description: "Unique identifer for the secondary token/cluster.",
			},
			"ttl": {
				Type:        schema.TypeString,
				ForceNew:    true,
				Optional:    true,
				Description: "Token time-to-live.",
			},
			"secondary_public_key": {
				Type:        schema.TypeString,
				ForceNew:    true,
				Optional:    true,
				Description: "Secondary cluster public key.",
			},
			"secondary_token": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Secondary token.",
			},
		},
	}
}

func replicationTokenCreatePath(typeValue string) string {
	return replicationPath + typeValue + "/primary/secondary-token"
}

func replicationTokenDeletePath(typeValue string) string {
	return replicationPath + typeValue + "/primary/revoke-secondary"
}

func replicationTokenCreate(d *schema.ResourceData, meta interface{}) error {
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return e
	}

	typeValue := d.Get("type").(string)
	path := replicationTokenCreatePath(typeValue)
	id := d.Get("token_id").(string)

	data := map[string]interface{}{
		"id": id,
	}
	if v, ok := d.GetOk("ttl"); ok {
		data["ttl"] = v.(string)
	}
	if v, ok := d.GetOk("secondary_public_key"); ok {
		data["secondary_public_key"] = v.(string)
	}

	resp, err := client.Logical().Write(path, data)
	if err != nil {
		return fmt.Errorf("error creating replication token: %s", err)
	}
	if resp == nil {
		log.Printf("[DEBUG] Response from client was nil")
	} else {
		if error, ok := resp.Data["Errors"]; ok {
			return fmt.Errorf("error creating replication token: %s", error)
		}
	}

	log.Printf("[DEBUG] Replication token created (%s)", typeValue)
	d.SetId(id)

	secondaryToken := resp.WrapInfo.Token
	d.Set("secondary_token", secondaryToken)

	return replicationTokenRead(d, meta)
}

func replicationTokenRead(d *schema.ResourceData, meta interface{}) error {
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

	return nil
}

func replicationTokenDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[DEBUG] Deleteing replication token")
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return e
	}

	typeValue := d.Get("type").(string)
	path := replicationTokenDeletePath(typeValue)

	data := map[string]interface{}{
		"id": d.Get("token_id"),
	}

	resp, err := client.Logical().Write(path, data)
	if err != nil {
		return fmt.Errorf("error revoking secondary token: %s", err)
	}

	if resp.Data["Errors"] != nil {
		return fmt.Errorf("error revoking secondary token: %s", resp.Data["Errors"])
	}

	return nil
}

func replicationTokenExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	log.Printf("[DEBUG] Checking if replication token exists")
	client, e := provider.GetClient(d, meta)
	if e != nil {
		return false, e
	}

	typeValue := d.Get("type").(string)
	path := replicationPrimaryReadPath(typeValue)

	resp, err := client.Logical().Read(path)
	if err != nil {
		return true, fmt.Errorf("error checking replication token: %s", err)
	}

	if resp.Data["Errors"] != nil {
		return true, fmt.Errorf("error checking replication token: %s", resp.Data["Errors"])
	}

	if resp.Data["secondaries"] != nil {
		secondaries := resp.Data["secondaries"].([]interface{})

		for _, z := range secondaries {
			for k, v := range z.(map[string]interface{}) {
				if k == "node_id" {
					if v.(string) == d.Get("token_id") {
						log.Printf("[DEBUG] Found replication token with id %s", v.(string))
						return true, nil
					}
				}
			}
		}
	}

	return false, nil
}
