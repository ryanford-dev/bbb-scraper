local cqueues = require "cqueues"
local request = require "http.request"
local gumbo = require "gumbo"
local sqlite3 = require "lsqlite3"
local argparse = require "argparse"
local colors = require "ansicolors"

local function slugify(str)
	return str:lower():gsub("%p", function(c)
		if c:match("_") then
			return "-"
		elseif not c:match("-") then
			return ""
		end
		return false
	end)
end

local function validate_state(str)
	if type(str) == "string" and #str == 2 and str:match("%a%a") then
		return str:lower()
	else
		print("enter valid 2 letter state code")
		os.exit(1)
	end
end

local parser = argparse(
	"BBB Scraper",
	"Pull business info from BBB based on location"
)
parser:option "-b" "--begin"
	:argname "<a>"
	:description "Letter to start scraping at"
	:default "a"
	:convert(string.lower)
parser:option "-e" "--end"
	:argname "<z>"
	:description "Letter to end scraping at"
	:default "z"
	:convert(string.lower)
parser:option "-s" "--state"
	:argname "<state>"
	:args(1)
	:description "State to find listings (2 letter designation)"
	:convert(validate_state)
parser:option "-c" "--city"
	:argname "<city>"
	:args("+")
	:description "City to find listings"
	:convert(slugify)
parser:option "-t" "--timeout"
	:argname "<seconds>"
	:description "Timeout value (seconds) on http requests"
	:default "10"
	:convert(tonumber)
parser:option "-o" "--output"
	:argname "<data.db>"
	:description "Sqlite3 DB to create/use"
	:default "data.db"
parser:mutex(
	parser:flag "-d" "--debug",
	parser:flag "-q" "--quiet"
)

local args = parser:parse()

if not (args.state and args.city) then
	if args.state and not args.city then
		print("enter a valid city")
	elseif args.city and not args.state then
		print("enter a valid state")
	else
		print("enter a valid city and state")
	end
	os.exit(1)
elseif type(args.city) == "table" then
	args.city = table.concat(args.city, "-")
end

local db = assert(sqlite3.open(args.output), "failed to open db")
assert(db:exec[[CREATE TABLE IF NOT EXISTS businesses(
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	address TEXT,
	phone TEXT,
	website TEXT,
	industry TEXT,
	tags TEXT
);]], db:errmsg())

local START_TIME = os.time()
local QUIET = args.quiet
local DEBUG = args.debug
local STATE, CITY = args.state, args.city
local HOST_URL = "https://www.bbb.org"
local TEMPLATE_URL = HOST_URL .. "/us/%s/%s/categories/%%s"
local BASE_URL = TEMPLATE_URL:format(STATE, CITY)
local OPEN_FILES = 0
local MAX_OPEN_FILES = 100
local entries = 0

local _print = print
local function print(...)
	if QUIET then return end
	return _print(...)
end

local function pp(mod, ...)
	local str = table.concat({...}, " ")
	return print(colors("%{" .. mod .. "}" .. str))
end

local alpha_loop = cqueues.new()
local alpha_s,alpha_e = args.begin:byte(), args["end"]:byte()

for i = 0, math.abs(alpha_e - alpha_s) do
	local alpha = string.char(math.min(alpha_s, alpha_e) + i)
	local alpha_url = BASE_URL:format(alpha)
	alpha_loop:wrap(function()
		local req = request.new_from_uri(alpha_url)
		if DEBUG then pp("blue", "fetching ", alpha_url) end
		while OPEN_FILES > MAX_OPEN_FILES do
			cqueues.sleep(0)
		end
		OPEN_FILES = OPEN_FILES + 1
		local headers, stream = req:go(args.timeout)
		if headers == nil then
			OPEN_FILES = OPEN_FILES - 1
			pp("red", "failed to fetch " .. alpha_url)
			pp("red", tostring(stream) .. "\n")
			pp("yellow", "skipping " .. alpha_url)
			return
		else
			local alpha_body, err = stream:get_body_as_string()
			if not alpha_body and err then
				OPEN_FILES = OPEN_FILES - 1
				pp("red", "failed to read body of " .. alpha_url)
				pp("red", tostring(err) .. "\n")
				pp("yellow", "skipping " .. alpha_url)
				return
			else
				stream:shutdown()
				OPEN_FILES = OPEN_FILES - 1
				local category_loop = cqueues.new()
				local alpha_document = assert(gumbo.parse(alpha_body), "failed to parse " .. alpha_url)
				local category_links = assert(alpha_document:getElementsByClassName("dtm-all-categories-category"), "no links found on " .. alpha_url)
				if DEBUG then print("found " .. #category_links .. " links on " .. alpha_url) end
				for _, link in ipairs(category_links) do
					local category_url = assert(link:getAttribute("href"), "couldn't get url for category link in " .. alpha)
					category_loop:wrap(function()
						local req = request.new_from_uri(HOST_URL .. category_url)
						while OPEN_FILES > MAX_OPEN_FILES do
							cqueues.sleep(0)
						end
						if DEBUG then pp("blue", "fetching " .. category_url) end
						OPEN_FILES = OPEN_FILES + 1
						local headers, stream = req:go(args.timeout)
						if headers == nil then
							OPEN_FILES = OPEN_FILES - 1
							pp("red", "failed to fetch " .. category_url)
							pp("red", tostring(stream) .. "\n")
							pp("yellow", "skipping " .. category_url)
							return
						else
							local category_body, err = stream:get_body_as_string()
							if not category_body and err then
								OPEN_FILES = OPEN_FILES - 1
								pp("red", "failed to read body of " .. category_url)
								pp("red", tostring(err) .. "/n")
								pp("yellow", "skipping " .. category_url)
								return
							else
								stream:shutdown()
								OPEN_FILES = OPEN_FILES - 1
								local listing_loop = cqueues.new()
								local category_document = assert(gumbo.parse(category_body), "failed to parse " .. category_url)
								local headings = assert(category_document:getElementsByTagName("h3"), "failed to find headings in " .. category_url)
								if DEBUG then print("found " .. #headings .. " in " .. category_url) end
								if #headings > 0 then
									for _, heading in ipairs(headings) do
										local listing_url do
											local anchor = heading:getElementsByTagName("a")
											if anchor then
												listing_url = anchor[1]:getAttribute("href")
											end
										end
										listing_loop:wrap(function()
											local req = request.new_from_uri(listing_url)
											while OPEN_FILES > MAX_OPEN_FILES do
												cqueues.sleep(0)
											end
											if DEBUG then pp("blue", "fetching " .. listing_url) end
											OPEN_FILES = OPEN_FILES + 1
											local headers, stream = req:go(args.timeout)
											if headers == nil then
												OPEN_FILES = OPEN_FILES - 1
												pp("red", "failed to fetch " .. listing_url)
												pp("red", tostring(stream) .. "\n")
												pp("yellow", "skipping " .. listing_url)
												return
											else
												local listing_body = stream:get_body_as_string()
												if not listing_body and err then
													OPEN_FILES = OPEN_FILES - 1
													pp("red", "failed to read body of " .. listing_url)
													pp("red", tostring(err) .. "/n")
													pp("yellow", "skipping " .. listing_url)
													return
												else
													stream:shutdown()
													OPEN_FILES = OPEN_FILES - 1
													local listing_document = assert(gumbo.parse(listing_body), "failed to parse " .. listing_url)
													if DEBUG then print("parsed " .. listing_url) end
													local divs = assert(listing_document:getElementsByTagName("div"), "failed to get divs in " .. listing_url)
													if DEBUG then print("found " .. #divs .. " divs on " .. listing_url) end
													local info = assert(listing_document:getElementsByClassName("business-card__content-container"))
													info = info and info[1]
													if DEBUG then
														if info then
															print("found business-card__content-container")
														else
															pp("red", "failed to find business-card__content-container")
															pp("yellow", "skipping " .. listing_url)
															return
														end
													end
													local contact = assert(listing_document:getElementsByClassName("business-contact-card"))
													contact = contact and contact[1]
													if DEBUG then
														if contact then
															print("found business-contact-card")
														else
															pp("red", "failed to find business-contact-card")
															pp("yellow", "skipping " .. listing_url)
															return
														end
													end
													local name = assert(info:getElementsByTagName("h4"))
													name = name and name[1]
													name = name and name.textContent
													if DEBUG and name then print("found name: " .. name) end
													local industry = assert(info:getElementsByTagName("h6"))
													industry = industry and industry[1]
													industry = industry and industry.textContent
													if DEBUG and industry then print("found industry: " .. industry) end
													local tags = info:getElementsByClassName("business-card__description")
													tags = tags and tags[1]
													tags = tags and tags.textContent
													if DEBUG and tags then print("found tags: " .. tags) end
													local address = contact:getElementsByClassName("dtm-address")
													address = address and address[1]
													address = address and address.textContent
													if DEBUG and address then print("found address for " .. name) end
													local website do
														local p = assert(contact:getElementsByClassName("dtm-url"))
														if #p > 0 then
															website = assert(p[1]:getAttribute("href"))
														end
													end
													if DEBUG then
														if website then
															print("found website: " .. website)
														else
															pp("yellow", "no website for " .. name)
														end
													end
													local phone_no do
														local p = assert(contact:getElementsByClassName("dtm-phone"))
														if #p > 0 then
															phone_no = p[1].textContent
														end
													end
													if DEBUG then
														if phone_no then
															print("found phone: " .. phone_no)
														else
															pp("yellow", "no phone for " .. name)
														end
													end

													local insert_stmt = assert(db:prepare"INSERT INTO businesses (name, address, phone, website, industry, tags) VALUES ($name, $address, $phone, $website, $industry, $tags);", db:errmsg())

													assert(insert_stmt:bind_names{
														name = name,
														address = address,
														phone = phone_no,
														website = website,
														industry = industry,
														tags = tags
													} == sqlite3.OK, db:errmsg())
													pp("green", "inserting " .. name .. " into db")
													assert(insert_stmt:step() == sqlite3.DONE, db:errmsg())
													assert(insert_stmt:finalize() == sqlite3.OK, db:errmsg())
													entries = entries + 1
												end
											end
										end)
									end

									if DEBUG then print("starting listing_loop " .. category_url) end
									listing_loop:loop()
								else
									pp("yellow", "no headings in " .. category_url)
								end
							end
						end
					end)
				end

				if DEBUG then print("starting category_loop " .. alpha_url) end
				category_loop:loop()
			end
		end
	end)
end

alpha_loop:loop()

local code = db:close()
assert(code == sqlite3.OK, "internal error: " .. code)
local dt = os.time() - START_TIME
local seconds = dt % 60
local minutes = math.floor(dt / 60) % 60
local hours = math.floor(dt / 60 / 60)
pp("green", "scraped " .. entries .. " entries")
pp("cyan", "scraping took: " .. hours .. "h " .. minutes .. "m " .. seconds .. "s")
os.exit(0, true)
