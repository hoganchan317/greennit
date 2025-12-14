-- Migration: create forums, forum_members, forum_requests
-- MySQL / MariaDB compatible SQL (utf8mb4)

CREATE TABLE IF NOT EXISTS `forums` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(191) NOT NULL,
  `description` TEXT,
  `creator_id` INT UNSIGNED NOT NULL,
  `is_private` TINYINT(1) NOT NULL DEFAULT 1,
  `is_approved` TINYINT(1) NOT NULL DEFAULT 0,
  `invite_code` VARCHAR(64) DEFAULT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  INDEX (`creator_id`),
  INDEX (`invite_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `forum_members` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `forum_id` INT UNSIGNED NOT NULL,
  `user_id` INT UNSIGNED NOT NULL,
  `role` ENUM('member','moderator','admin') NOT NULL DEFAULT 'member',
  `joined_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `forum_user_unique` (`forum_id`,`user_id`),
  INDEX (`forum_id`),
  INDEX (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `forum_requests` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(191) NOT NULL,
  `description` TEXT,
  `is_private` TINYINT(1) NOT NULL DEFAULT 1,
  `creator_id` INT UNSIGNED NOT NULL,
  `status` ENUM('pending','approved','rejected') NOT NULL DEFAULT 'pending',
  `admin_reply` TEXT DEFAULT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  INDEX (`creator_id`),
  INDEX (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Optionally add foreign keys if you maintain stricter referential integrity:
-- ALTER TABLE `forums` ADD CONSTRAINT `fk_forums_creator` FOREIGN KEY (`creator_id`) REFERENCES `users`(`id`) ON DELETE SET NULL;
-- ALTER TABLE `forum_members` ADD CONSTRAINT `fk_forum_members_forum` FOREIGN KEY (`forum_id`) REFERENCES `forums`(`id`) ON DELETE CASCADE;
-- ALTER TABLE `forum_members` ADD CONSTRAINT `fk_forum_members_user` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE;
-- ALTER TABLE `forum_requests` ADD CONSTRAINT `fk_forum_requests_creator` FOREIGN KEY (`creator_id`) REFERENCES `users`(`id`) ON DELETE SET NULL;
