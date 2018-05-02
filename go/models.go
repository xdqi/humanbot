package main

import "gopkg.in/guregu/null.v3"

type ChatNew struct {
	ID        int      `gorm:"column:id;AUTO_INCREMENT;primary_key" json:"id,omitempty"`
	ChatId    int64    `gorm:"column:chatid" json:"chat_id"`
	MessageId int64    `gorm:"column:messageid" json:"message_id"`
	UserId    null.Int `gorm:"column:userid" json:"user_id"`
	Text      string
	Date      int      `gorm:"column:time"`
	Flag      int16
}

func (ChatNew) TableName() string {
	return "chat_new"
}

type User struct {
	UID       int         `gorm:"column:uid;primary_key" json:"user_id"`
	Username  null.String `gorm:"column:name;size:32" json:"username"`
	FirstName null.String `gorm:"column:firstname;size:255" json:"first_name"`
	LastName  null.String `gorm:"column:lastname;size:255" json:"last_name"`
	LangCode  null.String `gorm:"column:lang;size:10" json:"lang_code"`
}

func (User) TableName() string {
	return "users"
}

type UsernameHistory struct {
	ID        int         `gorm:"column:id;AUTO_INCREMENT;primary_key"`
	UID       int         `gorm:"column:uid"`
	Username  null.String `gorm:"column:name;size:32"`
	FirstName null.String `gorm:"column:firstname;size:255"`
	LastName  null.String `gorm:"column:lastname;size:255"`
	LangCode  null.String `gorm:"column:lang;size:10"`
	Date      int
}

func (UsernameHistory) TableName() string {
	return "user_history"
}

type Group struct {
	GID    int64       `gorm:"column:id;primary_key" json:"chat_id"`
	Name   string      `gorm:"size:100"`
	Link   null.String `gorm:"size:50"`
	Master null.Int    `json:"master_uid"`
}

func (Group) TableName() string {
	return "groups"
}

type GroupHistory struct {
	ID   int         `gorm:"column:id;AUTO_INCREMENT;primary_key"`
	GID  int64
	Name string      `gorm:"size:100"`
	Link null.String `gorm:"size:50"`
	Date int
}

func (GroupHistory) TableName() string {
	return "group_history"
}
