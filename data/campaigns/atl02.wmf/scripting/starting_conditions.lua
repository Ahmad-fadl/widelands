-- =======================================================================
--                      Atlanteans Tutorial Mission 02
-- =======================================================================

-- ===============
-- Initialization
-- ===============

-- Initializing p1 (the scenario player)
   p1:allow_buildings("all")
   p1:forbid_buildings{"atlanteans_scouts_house"}
   p1:allow_workers("all")

   local sf = map.player_slots[1].starting_field
   local ship = p1:place_ship(map:get_field(41,0))
   ship.capacity = 110
   ship:make_expedition({
      atlanteans_fishbreeder = 1,
      atlanteans_geologist = 2,
      atlanteans_miner = 3,
      atlanteans_sawyer = 2,
      atlanteans_smelter = 1,
      atlanteans_stonecutter = 1,
      atlanteans_toolsmith = 1,
      atlanteans_woodcutter = 5,
      atlanteans_soldier = 8,
      atlanteans_forester =1,
      diamond = 1,
      coal = 9,
      quartz = 2,
      granite = 4,
      spider_silk = 4,
      smoked_fish = 10,
      smoked_meat = 10,
      atlanteans_bread = 20,
      log = 4,
      atlanteans_builder = 3,
      })

   ship = p1:place_ship(map:get_field(44,0))
   ship.capacity = 110
   ship:make_expedition({
      iron_ore = 40,
      spidercloth = 2,
      log = 29,
      planks = 20,
      atlanteans_builder = 1,
      })

-- Initialize Maletus (Player 2)
   Maletus:allow_buildings("all")

   sf = map.player_slots[2].starting_field
   Maletus:place_flag(sf.brn,true)
   create_economy_for_plr_2()

-- Initialize Kalitath (Player 3)
   Kalitath:allow_buildings("all")

   sf = map.player_slots[3].starting_field
   Kalitath:place_flag(sf.brn,true)
   create_economy_for_plr_3()
