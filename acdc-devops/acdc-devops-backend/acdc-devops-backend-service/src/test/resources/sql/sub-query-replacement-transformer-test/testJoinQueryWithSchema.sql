-- original
SELECT `t0`.`id` AS `userId`, `orders_22703`.`id` AS `orderId`
FROM (SELECT `users_22706`.`parent_id`, `users_22706`.`name`, `users_22706`.`id`, `users_22706`.`age`
FROM `default`.`users_22706` AS `users_22706`
WHERE `users_22706`.`name` = 'acdc') AS `t0`
LEFT JOIN `default`.`orders_22703` AS `orders_22703` ON `t0`.`id` = `orders_22703`.`user_id`

-- optimized
SELECT `t0`.`id` AS `userId`, `orders_22703`.`id` AS `orderId`
FROM (SELECT `users_22706`.`parent_id`, `users_22706`.`name`, `users_22706`.`id`, `users_22706`.`age`
FROM `default`.`users_22706` AS `users_22706`
WHERE `users_22706`.`name` = 'acdc') AS `t0`
LEFT JOIN (SELECT *
FROM `default`.`orders_22703` AS `orders_22703`) AS `orders_22703` ON `t0`.`id` = `orders_22703`.`user_id`