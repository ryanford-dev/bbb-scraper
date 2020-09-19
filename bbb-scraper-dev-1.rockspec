package = "bbb-scraper"
version = "0.0.0"
version = "dev-1"
source = {
   url = "",
	tag = "0.0.0"
}
dependencies = {
	"lua >= 5.3",
	"cqueues",
	"http",
	"gumbo",
	"lsqlite3",
	"argparse",
	"ansicolors"
}
build = {
   type = "builtin",
   modules = {
      ["bbb-scraper"] = "bbb-scraper.lua"
   }
}
