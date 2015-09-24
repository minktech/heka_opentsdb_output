# heka_opentsdb_output
OpenTSDB output plugin for heka

## Notice

if you want use this plugin in heka, please change **plugin_loader.cmake** like this

	add_external_plugin(git https://github.com/minktech/heka_opentsdb_plugin 22506252466c29ae22dbcce8541fafa624cce2cb)
	externalproject_add(
    	"bosun.org"
    	GIT_REPOSITORY "https://github.com/bosun-monitor/bosun.git"
    	GIT_TAG "ddc314f6b215ee053763fd08631d5995d1396c13"
    	SOURCE_DIR "${PROJECT_PATH}/src/bosun.org"
    	BUILD_COMMAND ""
    	CONFIGURE_COMMAND ""
    	INSTALL_COMMAND ""
    	UPDATE_COMMAND "" # comment out to enable updates
	)
	add_dependencies(GoPackages "bosun.org")