dirname = path.dirname(__file__)

tribes:new_productionsite_type {
   name = "empire_marblemine_deep",
   -- TRANSLATORS: This is a building name used in lists of buildings
   descname = _"Deep Marble Mine",
   size = "mine",
   buildable = false,
   enhanced_building = true,

   enhancement_cost = {
		log = 4,
		planks = 2
	},
	return_on_dismantle_on_enhanced = {
		log = 2,
		planks = 1
	},

	-- TRANSLATORS: Helptext for a building: Deep Marble Mine
   helptext = "", -- NOCOM(GunChleoc): See what we can shift over from help.lua here

   animations = {
		idle = {
			pictures = { dirname .. "idle_\\d+.png" },
			hotspot = { 49, 61 },
		},
		working = {
			pictures = { dirname .. "working_\\d+.png" },
			hotspot = { 49, 61 },
			fps = 10
		},
		empty = {
			pictures = { dirname .. "empty_\\d+.png" },
			hotspot = { 49, 61 },
		},
	},

   aihints = {
		mines = "granite"
   },

	working_positions = {
		empire_miner = 1,
		empire_miner_master = 1
	},

   inputs = {
		meal = 6,
		wine = 6
	},
   outputs = {
		"marble",
		"granite"
   },

	programs = {
		work = {
			-- TRANSLATORS: Completed/Skipped/Did not start mining marble because ...
			descname = _"mining marble",
			actions = {
				"sleep=43000",
				"return=skipped unless economy needs marble or economy needs granite",
				"consume=meal wine",
				"animate=working 18000",
				"mine=granite 2 100 5 2",
				"produce=marble:2",
				"animate=working 18000",
				"mine=granite 2 100 5 2",
				"produce=marble granite:2"
			}
		},
	},
	out_of_resource_notification = {
		title = _"Main Marble Vein Exhausted",
		message =
			_"This marble mine’s main vein is exhausted. Expect strongly diminished returns on investment." .. " " ..
			-- TRANSLATORS: "it" is a mine.
			_"This mine can’t be enhanced any further, so you should consider dismantling or destroying it.",
		delay_attempts = 0
	},
}
