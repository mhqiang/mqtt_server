package mqtt_server

import "github.com/allegro/bigcache"

type UserInfo struct {
}

type UserCache struct {
	UserMap *bigcache.BigCache
}

func (user *UserCache) InitUserCache() userCache *UserCache {
	return 
}

// 
func (user *UserCache) AddUser() err error {
	return
}

// del uer
func (user *UserCache) DelUser() err error {
	return
}

// update User
func (user *UserCache) UpdateUser()  err error {
	return
}

func (user *UserCache) AuthUser(user,password string) (bool, error){
	return true, nil
}