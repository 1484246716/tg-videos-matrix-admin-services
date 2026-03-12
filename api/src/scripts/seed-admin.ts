import 'dotenv/config';
import { PrismaClient } from '@prisma/client';
import * as bcrypt from 'bcryptjs';

const prisma = new PrismaClient();

async function main() {
    const args = process.argv.slice(2);

    const usernameIndex = args.indexOf('--username');
    const passwordIndex = args.indexOf('--password');

    if (usernameIndex === -1 || passwordIndex === -1) {
        console.error('Usage: ts-node src/scripts/seed-admin.ts --username <name> --password <pass>');
        process.exit(1);
    }

    const username = args[usernameIndex + 1];
    const password = args[passwordIndex + 1];

    if (!username || !password) {
        console.error('Error: --username and --password values are required');
        process.exit(1);
    }

    if (password.length < 8) {
        console.error('Error: password must be at least 8 characters');
        process.exit(1);
    }

    const passwordHash = await bcrypt.hash(password, 12);

    const user = await prisma.user.upsert({
        where: { username },
        update: {
            passwordHash,
            role: 'admin',
            status: 'active',
        },
        create: {
            username,
            passwordHash,
            role: 'admin',
            status: 'active',
        },
    });

    console.log(`✅ Admin account ready: username="${user.username}", id=${user.id.toString()}`);
}

main()
    .catch((err) => {
        console.error('❌ seed-admin failed:', err);
        process.exit(1);
    })
    .finally(() => prisma.$disconnect());
