package user

import (
	//"os"
	//"log"
	"time"
	"strings"
	"sort"
	"strconv"
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/gocql/gocql"

	common "app/common"
)

//var logger = log.New(os.Stderr, "", log.LstdFlags)

type User struct {
	Login string                      `json:"login"`
	Password string                   `json:"password"`
	Name string                       `json:"name"`
	Birthdate json.Number             `json:"birthdate"`
	AvatarUrls []string               `json:"avatarUrls"`
	SocialCodes map[string]string     `json:"socialCodes"`
	Emails []string                   `json:"emails"`
	Phones []string                   `json:"phones"`
	Created json.Number               `json:"created"`
	Modified json.Number              `json:"modified"`
	State json.Number                 `json:"state"`
	Latitude json.Number              `json:"latitude"`
	Longitude json.Number             `json:"longitude"`
}

type _UserInsertData struct {
	Login string
	Password string
	Name string
	Birthdate time.Time
	AvatarUrls []string
	SocialCodes map[string]string
	Emails []string
	Phones []string
	Created time.Time
	Modified time.Time
	State int
	Latitude float32
	Longitude float32
}

func (u *User) SetState(state common.UserState) {
	u.State = json.Number(strconv.FormatInt(int64(state), 10))
}

func (u *User) Insert(session *gocql.Session, producer sarama.SyncProducer, data *string) error {
	var err error
	if err = u.insert_user(session); err != nil {
		return err
	}
	return nil
}

func (u *User) Update(session *gocql.Session, producer sarama.SyncProducer, data *string) error {
	var err error
	if err = u.updateHelper(session, data); err != nil {
		return err
	}

	return nil
}

func (u *User) Remove(session *gocql.Session, producer sarama.SyncProducer, data *string) error {
	var err error

	u.SetState(common.UserStateRemoved);
	if err = u.updateHelper(session, data); err != nil {
		return err
	}

	return nil
}

//==============================================================================
//
// Private members
//
//==============================================================================

func (u *User) updateHelper(session *gocql.Session, data *string) error {
	var err error
	var fields *map[string]interface{}
	if fields, err = u.getUpdateFields(); err != nil {
		return err
	}

	if len(*fields) == 0 {
		return nil
	}
	updateSetValues := getUpdateSetValues(fields)
	whereValuesByLogin := u.getWhereValuesByLogin()
	updateSetStatement := calcUpdateSetStatement(fields)
	paramsByLogin := append(updateSetValues, whereValuesByLogin...)

	if err = session.Query(
		"UPDATE user SET " + updateSetStatement +
		" WHERE u_login=?", 
		paramsByLogin...
	).Exec(); err != nil {
		return err
	}

	return nil
}

func (u *User) getInsertData() (*_UserInsertData, error) {
	var err error

	var latitude float32
	if "" != u.Latitude.String() {
		var latitude0 float64
		if latitude0, err = u.Latitude.Float64(); nil != err {
			return nil, err
		}
		latitude = float32(latitude0)
	}
	var longitude float32
	if "" != u.Longitude.String() {
		var longitude0 float64
		if longitude0, err = u.Longitude.Float64(); nil != err {
			return nil, err
		}
		longitude = float32(longitude0)
	}

	var birthdate int64
	if "" != u.Birthdate.String() {
		if birthdate, err = u.Birthdate.Int64(); nil != err {
			return nil, err
		}
	}

	var now time.Time
	var created int64
	if "" != u.Created.String() {
		if created, err = u.Created.Int64(); nil != err {
			return nil, err
		}
	} else {
		now = time.Now()
		created = now.Unix() * 1000 // milliseconds
		u.Created = json.Number(strconv.FormatInt(created, 10))
	}

	var modified int64
	if "" != u.Modified.String() {
		if modified, err = u.Modified.Int64(); nil != err {
			return nil, err
		}
	} else {
		modified = created
		u.Modified = json.Number(strconv.FormatInt(modified, 10))
	}

	var state int64
	if "" != u.State.String() {
		if state, err = u.State.Int64(); nil != err {
			return nil, err
		}
	}

	return &_UserInsertData{
		u.Login,
		u.Password,
		u.Name,
		time.Unix(birthdate / 1000, 0),
		u.AvatarUrls,
		u.SocialCodes,
		u.Emails,
		u.Phones,
		time.Unix(created / 1000, 0),
		time.Unix(modified / 1000, 0),
		int(state),
		latitude,
		longitude,
	}, nil
}

func (u *User) insert_user(session *gocql.Session) error {
	var err error
	var d *_UserInsertData
	if d, err = u.getInsertData(); nil != err {
		return err
	}

	return session.Query(`
		INSERT INTO user (
			u_login,
			u_password,
			u_name,
			u_birthdate,
			u_avatarUrl,

			u_social_code,
			u_email,
			u_phone,
			u_created,
			u_modified,

			u_state
		) VALUES (
			?, ?, ?, ?, ?, 
			?, ?, ?, ?, ?, 
			?
		)`, 
		d.Login,
		d.Password,
		d.Name,
		d.Birthdate,
		d.AvatarUrls,

		d.SocialCodes,
		d.Emails,
		d.Phones,
		d.Created,
		d.Modified,

		d.State,
	).Exec()
}

func (u *User) getWhereValuesByLogin() []interface{} {
	s := make([]interface{}, 1)

	login := u.Login

	s[0] = login

	return s
}

func (u *User) getUpdateFields() (*map[string]interface{}, error) {
	var fields map[string]interface{} = make(map[string]interface{})

	var err error
	var d *_UserInsertData
	if d, err = u.getInsertData(); nil != err {
		return nil, err
	}

	if "" != u.Password {
		fields["password"] = u.Password
	}

	if "" != u.Name {
		fields["name"] = u.Name
	}

	if "" != u.Birthdate {
		fields["birthdate"] = d.Birthdate
	}

	if nil != u.AvatarUrls {
		fields["avatarUrl"] = d.AvatarUrls
	}

	if nil != u.SocialCodes {
		fields["social_code"] = d.SocialCodes
	}

	if nil != u.Emails {
		fields["email"] = d.Emails
	}

	if nil != u.Phones {
		fields["phone"] = d.Phones
	}

	if "" != u.State.String() {
		fields["state"] = d.State
	}

	fields["modified"] = d.Modified

	return &fields, nil
}

//==============================================================================
//
// Helper functions
//
//==============================================================================

func getFieldsNames(fields *map[string]interface{}) []string {
	names := make([]string, 0, len(*fields))
	for i := range *fields {
		names = append(names, i)
	}
	sort.Strings(names)
	return names
}

func getFieldsValues(fields *map[string]interface{}) []interface{} {
	values := make([]interface{}, 0)
	names := getFieldsNames(fields)
	f := *fields
	for _, i := range names {
		values = append(values, f[i])
	}
	return values
}

func calcUpdateSetStatement(fields *map[string]interface{}) string {
	s := make([]string, 0)

	names := getFieldsNames(fields)
	for _, name := range names {
		s = append(s, "u_" + name + "=?")
	}

	return strings.Join(s, ",")
}

func getUpdateSetValues(fields *map[string]interface{}) []interface{} {
	s := make([]interface{}, 0)

	values := getFieldsValues(fields)
	for _, value := range values {
		s = append(s, value)
	}

	return s
}
